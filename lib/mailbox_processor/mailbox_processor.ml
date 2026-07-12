(** A request language used to preserve call result types through the queue. *)
module type Request = sig
  type _ t
end

(** Implements a bounded FIFO processor for one request language. *)
module Make (Request : Request) = struct
  (** A reason why work cannot complete. *)
  type failure = Closed | Handler_raised of exn

  (** The handler's universal field preserves the result type selected by each
      GADT request without using an untyped representation cast. *)
  type handler = { handle : 'result. 'result Request.t -> 'result }

  (** A one-shot result cell shared by one caller and the owner Domain. All
      access to [outcome] is protected by [mutex]. *)
  type 'result reply = {
    mutex : Mutex.t;
    ready : Condition.t;
    mutable outcome : ('result, failure) result option;
  }

  (** Opaque caller-side capability for awaiting one already admitted terminal
      request. Constructing this value is proof that admission has closed. *)
  type 'result pending = 'result reply

  (** An existential queue entry. [None] marks a post; [Some reply] marks a
      call whose result type is tied to the request by this constructor. *)
  type job = Job : 'result Request.t * 'result reply option -> job

  (** Lifecycle values protected by the processor mutex. [Closing] drains the
      queue, while [Failed] has already removed it and settled every call. *)
  type lifecycle = Open | Closing | Failed of exn | Stopped

  (** Shared processor state. [mutex] establishes the total enqueue order and
      protects [jobs] and [lifecycle]. [join_mutex] makes joining idempotent. *)
  type t = {
    capacity : int;
    handler : handler;
    jobs : job Queue.t;
    mutex : Mutex.t;
    has_jobs : Condition.t;
    has_capacity : Condition.t;
    mutable lifecycle : lifecycle;
    mutable owner : unit Domain.t option;
    join_mutex : Mutex.t;
    mutable joined : (unit, failure) result option;
  }

  (** Runs [operation] while holding [mutex] and releases it even if an
      asynchronous or internal exception interrupts the critical section. *)
  let with_mutex mutex operation =
    Mutex.lock mutex;
    Fun.protect ~finally:(fun () -> Mutex.unlock mutex) operation

  (** Allocates an unresolved one-shot reply cell. *)
  let create_reply () =
    {
      mutex = Mutex.create ();
      ready = Condition.create ();
      outcome = None;
    }

  (** Resolves [reply] exactly once and wakes its caller. Duplicate settlement
      is an internal invariant violation. *)
  let settle (type result) (reply : result reply) outcome =
    with_mutex reply.mutex (fun () ->
        match reply.outcome with
        | None ->
            reply.outcome <- Some outcome;
            Condition.broadcast reply.ready
        | Some _ -> invalid_arg "mailbox reply settled twice")

  (** Waits for a reply, rechecking the predicate after every possible
      spurious wake. *)
  let await_reply (type result) (reply : result reply) =
    with_mutex reply.mutex (fun () ->
        while Option.is_none reply.outcome do
          Condition.wait reply.ready reply.mutex
        done;
        Option.get reply.outcome)

  (** Converts a protected lifecycle value into the admission error visible to
      producers. *)
  let admission_failure = function
    | Open -> invalid_arg "open mailbox has no admission failure"
    | Closing | Stopped -> Closed
    | Failed exn -> Handler_raised exn

  (** Adds [job] at the FIFO tail. The successful mutation is the operation's
      linearization point. Mutex ordering preserves each producer's program
      order; concurrently contending producers are ordered only by these
      successful enqueue mutations. *)
  let enqueue processor job =
    with_mutex processor.mutex (fun () ->
        while
          processor.lifecycle = Open
          && Queue.length processor.jobs >= processor.capacity
        do
          Condition.wait processor.has_capacity processor.mutex
        done;
        match processor.lifecycle with
        | Open ->
            Queue.add job processor.jobs;
            Condition.signal processor.has_jobs;
            Ok ()
        | lifecycle -> Error (admission_failure lifecycle))

  (** Admits exactly one terminal request while closing normal admission under
      the same mutex. The reserved extra queue position is intentional: a
      lifecycle request must never compete with ordinary producers for
      capacity, because that would let later work overtake or starve closure. *)
  let enqueue_and_close processor job =
    with_mutex processor.mutex (fun () ->
        match processor.lifecycle with
        | Open ->
            Queue.add job processor.jobs;
            processor.lifecycle <- Closing;
            Condition.signal processor.has_jobs;
            Condition.broadcast processor.has_capacity;
            Ok ()
        | lifecycle -> Error (admission_failure lifecycle))

  (** Settles the call carried by [job], if any, with terminal [failure]. Posts
      have no waiter and are deterministically discarded. *)
  let fail_job failure (Job (_, reply)) =
    match reply with None -> () | Some reply -> settle reply (Error failure)

  (** Atomically changes an open or closing processor to [Failed], removes all
      queued work, wakes blocked producers, then releases queued call waiters.
      Reply settlement happens after releasing the queue mutex to keep the two
      locking domains independent. *)
  let fail_processor processor exn =
    let jobs =
      with_mutex processor.mutex (fun () ->
          let jobs = ref [] in
          (match processor.lifecycle with
          | Open | Closing ->
              processor.lifecycle <- Failed exn;
              while not (Queue.is_empty processor.jobs) do
                jobs := Queue.take processor.jobs :: !jobs
              done;
              Condition.broadcast processor.has_capacity;
              Condition.broadcast processor.has_jobs
          | Failed _ | Stopped -> ());
          List.rev !jobs)
    in
    let failure = Handler_raised exn in
    List.iter (fail_job failure) jobs

  (** Removes the FIFO head for the owner. When close has drained the queue,
      this transition records orderly termination and returns [None]. *)
  let take processor =
    with_mutex processor.mutex (fun () ->
      while processor.lifecycle = Open && Queue.is_empty processor.jobs do
        Condition.wait processor.has_jobs processor.mutex
      done;
      if not (Queue.is_empty processor.jobs) then (
        let job = Queue.take processor.jobs in
        Condition.broadcast processor.has_capacity;
        Some job)
      else
        match processor.lifecycle with
        | Closing ->
            processor.lifecycle <- Stopped;
            Condition.broadcast processor.has_capacity;
            None
        | Failed _ | Stopped -> None
        | Open -> invalid_arg "open mailbox woke without a job"
    )

  (** Settles [reply] only when it is still unresolved. Used when an
      asynchronous exception may have interrupted the owner after dequeue but
      before the normal settlement path ran, so double-settle is not fatal. *)
  let settle_if_unresolved (type result) (reply : result reply) outcome =
    with_mutex reply.mutex (fun () ->
        match reply.outcome with
        | None ->
            reply.outcome <- Some outcome;
            Condition.broadcast reply.ready
        | Some _ -> ())

  (** Invokes one typed request. An unexpected exception settles the active
      call before failing and draining the processor, so no caller can be
      stranded between those two state changes. *)
  let handle_job processor (Job (request, reply)) =
    match processor.handler.handle request with
    | result ->
        Option.iter (fun reply -> settle reply (Ok result)) reply;
        true
    | exception exn ->
        let failure = Handler_raised exn in
        Option.iter (fun reply -> settle reply (Error failure)) reply;
        fail_processor processor exn;
        false

  (** Runs the sole consumer loop on the dedicated owner Domain. Tracks the
      dequeued job so an async exception between [take] and settlement cannot
      leave [await_reply] blocked forever. *)
  let rec run_owner processor inflight =
    match take processor with
    | None -> inflight := None
    | Some job ->
        inflight := Some job;
        if handle_job processor job then (
          inflight := None;
          run_owner processor inflight)
        else inflight := None

  (** Contains any unexpected processor-internal exception that escapes the
      owner loop and applies the same terminal cleanup as a handler defect.
      The dequeued in-flight call is settled first when still unresolved. *)
  let run_owner_guarded processor =
    let inflight = ref None in
    match run_owner processor inflight with
    | () -> ()
    | exception exn ->
        (match !inflight with
        | Some (Job (_, Some reply)) ->
            settle_if_unresolved reply (Error (Handler_raised exn))
        | Some (Job (_, None)) | None -> ());
        inflight := None;
        fail_processor processor exn

  (** Creates shared state before spawning the owner, then publishes the Domain
      handle before returning the processor to producers. *)
  let create ~capacity ~handler =
    if capacity <= 0 then
      invalid_arg "mailbox capacity must be greater than zero";
    let processor =
      {
        capacity;
        handler;
        jobs = Queue.create ();
        mutex = Mutex.create ();
        has_jobs = Condition.create ();
        has_capacity = Condition.create ();
        lifecycle = Open;
        owner = None;
        join_mutex = Mutex.create ();
        joined = None;
      }
    in
    let owner = Domain.spawn (fun () -> run_owner_guarded processor) in
    processor.owner <- Some owner;
    processor

  (** Admits a fire-and-forget unit request. *)
  let post processor request = enqueue processor (Job (request, None))

  (** Admits a typed call and waits for its one-shot result. *)
  let call processor request =
    let reply = create_reply () in
    match enqueue processor (Job (request, Some reply)) with
    | Error failure -> Error failure
    | Ok () -> await_reply reply

  (** Atomically admits the final call and returns before its handler runs. *)
  let submit_and_close processor request =
    let reply = create_reply () in
    match enqueue_and_close processor (Job (request, Some reply)) with
    | Error failure -> Error failure
    | Ok () -> Ok reply

  (** Waits for one request which has already crossed its admission point. *)
  let await pending = await_reply pending

  (** Admits a final typed call, closes admission, and waits for its result. *)
  let call_and_close processor request =
    match submit_and_close processor request with
    | Error failure -> Error failure
    | Ok pending -> await pending

  (** Linearizes close under the queue mutex and wakes both an idle owner and
      producers waiting for capacity. *)
  let close processor =
    with_mutex processor.mutex (fun () ->
        (match processor.lifecycle with
        | Open -> processor.lifecycle <- Closing
        | Closing | Failed _ | Stopped -> ());
        Condition.broadcast processor.has_jobs;
        Condition.broadcast processor.has_capacity)

  (** Reads the terminal result after [Domain.join] establishes that every
      owner write happens before this caller. *)
  let terminal_result processor =
    with_mutex processor.mutex (fun () ->
        match processor.lifecycle with
        | Failed exn -> Error (Handler_raised exn)
        | Stopped -> Ok ()
        | Open | Closing -> invalid_arg "joined mailbox is not terminal")

  (** Joins the owner at most once and caches its terminal result for later
      callers. *)
  let join processor =
    with_mutex processor.join_mutex (fun () ->
      match processor.joined with
      | Some result -> result
      | None ->
          let owner = Option.get processor.owner in
          Domain.join owner;
          let result = terminal_result processor in
          processor.joined <- Some result;
          result)
end
