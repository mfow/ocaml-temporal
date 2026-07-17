(** Implements typed client handles over the private semantic backend. *)

(** The client state is intentionally opaque in the public interface. A single
    backend value owns all native resources for this SDK instance. *)
type t = {
  (* The validated Temporal namespace used for every operation on this client. *)
  namespace : string;
  (* The backend owns the transport and native supervisor graph; no other
     client field retains a native handle. *)
  backend : Backend.client;
  (* Set before teardown begins so new operations fail without entering the
     backend after the lifecycle transition has been admitted. *)
  closed : bool Atomic.t;
  (* Serializes the first teardown with later callers that need the cached
     result, while backend shutdown itself remains outside public state. *)
  shutdown_mutex : Mutex.t;
  (* The first shutdown outcome is retained so every caller observes the same
     terminal result, including a native teardown error. *)
  mutable shutdown_result : (unit, Error.t) result option;
}

(** A handle retains the definition codecs and exact execution identity. *)
type ('input, 'output) handle = {
  (* The owning client keeps the backend alive for all operations on this
     handle; shutdown is still explicit and invalidates future calls. *)
  client : t;
  (* The workflow definition supplies the output codec used by [wait] and the
     name used when [start] builds the backend request. *)
  workflow : ('input, 'output) Workflow.t;
  (* The durable ID selected by the caller and echoed by the start response. *)
  workflow_id : string;
  (* The exact server run ID returned by Temporal; waits never follow a
     continued-as-new successor implicitly. *)
  run_id : string;
}

(** Identifies a successor execution returned by Temporal after a workflow
    continues as new. The pair is intentionally kept separate from a typed
    [handle]: callers must supply the original workflow definition when they
    turn this wire-level identity back into a handle, so the output codec is
    never guessed from an untyped run ID. *)
type execution = {
  (* Namespace that owns the successor execution. *)
  namespace : string;
  (* Durable workflow identity shared by the original and successor runs. *)
  workflow_id : string;
  (* Server-issued identity of the successor run. *)
  run_id : string;
}

(** Terminal outcomes mirror the backend while replacing payload bytes with the
    definition's typed output. *)
type 'output terminal_result =
  (* The terminal payload decoded with the workflow definition's output codec. *)
  | Completed of 'output
  (* Temporal reported a workflow failure as a typed terminal value. *)
  | Failed of Error.t
  (* The exact run accepted a cancellation request and reached cancellation. *)
  | Cancelled of Error.t
  (* The exact run was terminated by an operator or another Temporal client. *)
  | Terminated of Error.t
  (* The exact run reached a Temporal timeout terminal state. *)
  | Timed_out of Error.t
  (* The run continued as a new execution; callers choose whether to follow it. *)
  | Continued_as_new of execution

(** One execution row returned by the Temporal visibility service. *)
type visibility_execution = {
  workflow_id : string;
  run_id : string;
  workflow_type : string;
  task_queue : string;
  status : string;
}

(** A bounded visibility page and its opaque continuation token. *)
type visibility_page = {
  executions : visibility_execution list;
  next_page_token : string option;
}

(** The default identity is stable and descriptive without using process-global
    randomness, which keeps client construction straightforward in tests. *)
let default_identity = "ocaml-temporal-client"

(** All public client values in one process share this allocator. Signal
    request IDs are Temporal idempotency keys, so allocating from the client
    record would allow two independent handles to generate the same first ID
    for one exact execution. The atomic counter keeps concurrent callers
    distinct without introducing another lock around the native graph. *)
let next_signal_request_id = Atomic.make 0

(** Rejects empty, oversized, or NUL-containing identifiers before they can
    enter a backend request. The 65,536-byte bound is shared by the JSON
    protocol and native bridge, so mock and native transports reject the same
    malformed operation rather than diverging at their respective boundaries. *)
let validate_name field value =
  if String.equal value "" then
    Error (Error.defect ~message:(field ^ " must not be empty"))
  else if String.length value > 65_536 then
    Error
      (Error.defect
         ~message:(field ^ " exceeds the protocol string safety limit"))
  else if String.contains value '\000' then
    Error (Error.defect ~message:(field ^ " must not contain NUL"))
  else Ok ()

(** Builds the private backend configuration after checking every user-facing
    connection field. Routine configuration failures remain [result] values. *)
let create ?(identity = default_identity) ~target_url ~namespace () =
  match validate_name "namespace" namespace with
  | Error error -> Error error
  | Ok () -> (
      match validate_name "identity" identity with
      | Error error -> Error error
      | Ok () ->
          let config : Backend.config =
            { target_url; namespace; identity; task_queue = None }
          in
          Result.map
            (fun backend ->
              {
                namespace;
                backend;
                closed = Atomic.make false;
                shutdown_mutex = Mutex.create ();
                shutdown_result = None;
              })
            (Backend.client_create config))

(** Validates the optional Temporal idempotency key, workflow type, durable
    workflow ID, and task queue before encoding input or constructing a native
    request. Keeping these checks here makes malformed caller input a typed
    result and prevents it from crossing the supervisor boundary. *)
let validate_start_fields ~request_id ~workflow_name ~id ~task_queue =
  let request_result =
    match request_id with
    | None -> Ok ()
    | Some request_id -> validate_name "request id" request_id
  in
  match request_result with
  | Error _ as error -> error
  | Ok () -> (
      match validate_name "workflow type" workflow_name with
      | Error _ as error -> error
      | Ok () -> (
          match validate_name "workflow id" id with
          | Error _ as error -> error
          | Ok () -> validate_name "task queue" task_queue))

(** Checks metadata keys before they reach the protocol map representation.
    Rejecting duplicates here keeps caller-visible behavior independent of
    Rust's map implementation and avoids silently losing one value. *)
let validate_metadata_fields label fields =
  let rec loop seen = function
    | [] -> Ok ()
    | (key, _value) :: rest ->
        if List.mem key seen then
          Error
            (Error.make ~category:`Defect
               ~message:(Printf.sprintf "duplicate %s key %S" label key) ())
        else if String.equal key "" || String.contains key '\000' then
          Error
            (Error.make ~category:`Defect
               ~message:(Printf.sprintf "invalid %s key" label) ())
        else if String.length key > 65_536 then
          Error
            (Error.make ~category:`Defect
               ~message:(Printf.sprintf "%s key exceeds protocol limit" label) ())
        else loop (key :: seen) rest
  in
  loop [] fields

(** Starts a workflow after encoding its typed input and checking the backend's
    response still refers to the request. The response check prevents an
    adapter bug from creating a handle for a different execution. *)
let start client ?request_id ?(memo = []) ?(search_attributes = []) ~workflow
    ~task_queue ~id ~input () =
  if Atomic.get client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    match
      validate_start_fields ~request_id ~workflow_name:(Workflow.name workflow)
        ~id ~task_queue
    with
    | Error error -> Error error
    | Ok () -> (
        match validate_metadata_fields "memo" memo with
        | Error error -> Error error
        | Ok () -> (
          match validate_metadata_fields "search attribute" search_attributes with
          | Error error -> Error error
          | Ok () ->
            match Codec.encode (Workflow.input workflow) input with
        | Error error -> Error error
        | Ok encoded_input ->
            let request : Backend.start_request =
              {
                request_id;
                workflow_name = Workflow.name workflow;
                workflow_id = id;
                task_queue;
                input = encoded_input;
                memo;
                search_attributes;
              }
            in
            Result.bind (Backend.client_start client.backend request) (fun response ->
                if not (String.equal response.workflow_id id) then
                  Error
                    (Error.make ~category:`Bridge
                       ~message:"backend returned a different workflow id" ())
                else if String.equal response.run_id "" then
                  Error
                    (Error.make ~category:`Bridge
                       ~message:"backend returned an empty run id" ())
                else
                  Ok
                    {
                      client;
                      workflow;
                      workflow_id = id;
                      run_id = response.run_id;
                    })))

(** Rebuilds a typed handle for a successor run without starting another
    execution. Temporal returns a continuation's workflow/run identity in the
    terminal result for the original run; this operation validates that
    identity at the same boundary as [start], retains the caller's client and
    supplied workflow codecs, and leaves the exact-run choice explicit to the
    caller. Namespace equality is checked before constructing a handle so an
    execution returned by one client cannot be waited through another client's
    namespace. *)
let follow client ~workflow ({ namespace; workflow_id; run_id } : execution) =
  if Atomic.get client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    match validate_name "successor namespace" namespace with
    | Error error -> Error error
    | Ok () -> (
        if not (String.equal namespace client.namespace) then
          Error
            (Error.defect
               ~message:"successor execution belongs to a different namespace")
        else
          match validate_name "successor workflow id" workflow_id with
          | Error error -> Error error
          | Ok () -> (
              match validate_name "successor run id" run_id with
              | Error error -> Error error
              | Ok () -> Ok { client; workflow; workflow_id; run_id }))

(** Decodes a completed payload and maps terminal failures without exposing the
    private backend constructors. *)
let wait handle =
  if Atomic.get handle.client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    let request : Backend.wait_request =
      { workflow_id = handle.workflow_id; run_id = handle.run_id }
    in
    Result.bind (Backend.client_wait handle.client.backend request) (function
      | Backend.Completed payload ->
          Result.map
            (fun output -> Completed output)
            (Codec.decode (Workflow.output handle.workflow) payload)
      | Backend.Failed error -> Ok (Failed error)
      | Backend.Cancelled error -> Ok (Cancelled error)
      | Backend.Terminated error -> Ok (Terminated error)
      | Backend.Timed_out error -> Ok (Timed_out error)
      | Backend.Continued_as_new { workflow_id; run_id } ->
          Ok
            (Continued_as_new
               { namespace = handle.client.namespace; workflow_id; run_id }))

(** Validates cancellation metadata before it reaches the backend. The length
    limit protects the JSON bridge and matches the Rust-side bound; NUL is
    rejected because it cannot be represented safely by the C ABI contract. *)
let validate_cancel_fields ~request_id ~reason =
  let request_result =
    match request_id with
    | None -> Ok ()
    | Some request_id -> validate_name "cancellation request id" request_id
  in
  match request_result with
  | Error _ as error -> error
  | Ok () when String.length reason > 65_536 ->
      Error
        (Error.defect
           ~message:"cancellation reason exceeds the protocol safety limit")
  | Ok () when String.contains reason '\000' ->
      Error (Error.defect ~message:"cancellation reason must not contain NUL")
  | Ok () -> Ok ()

(** Validates signal metadata before encoding input or entering the backend.
    Signal names are validated when their definitions are built; the request
    ID still belongs to this particular delivery and is checked here. *)
let validate_signal_fields ~request_id =
  let request_result =
    match request_id with
    | None -> Ok ()
    | Some request_id -> validate_name "signal request id" request_id
  in
  request_result

(** Allocates a stable request ID for a cancellation call whose caller did not
    provide one. Hashing the exact execution identity makes repeated calls on
    the same handle represent one idempotent control operation without keeping
    another mutable counter in the client state. *)
let generated_cancel_request_id (handle : ('input, 'output) handle) =
  "ocaml-client-cancel-"
  ^ Digest.to_hex
      (Digest.string (handle.workflow_id ^ "\000" ^ handle.run_id))

(** Sends a cancellation request for one exact run and returns only after the
    server acknowledgement has been decoded. This operation is deliberately
    separate from [wait], because Temporal cancellation is asynchronous. *)
let cancel ?request_id ?(reason = "") handle =
  if Atomic.get handle.client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    match validate_cancel_fields ~request_id ~reason with
    | Error error -> Error error
    | Ok () ->
        let request_id =
          match request_id with
          | Some request_id -> request_id
          | None -> generated_cancel_request_id handle
        in
        let request : Backend.cancel_request =
          {
            workflow_id = handle.workflow_id;
            run_id = handle.run_id;
            request_id;
            reason;
          }
        in
        Backend.client_cancel handle.client.backend request

(** Validates operator reason text before it crosses either the deterministic
    mock or native JSON bridge. *)
let validate_terminate_reason reason =
  if String.length reason > 65_536 then
    Error
      (Error.defect
         ~message:"termination reason exceeds the protocol safety limit")
  else if String.contains reason '\000' then
    Error (Error.defect ~message:"termination reason must not contain NUL")
  else Ok ()

(** Terminates one exact run. The acknowledgement is deliberately separate
    from [wait], which observes the server's terminal history event. *)
let terminate ?(reason = "") handle =
  if Atomic.get handle.client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    match validate_terminate_reason reason with
    | Error error -> Error error
    | Ok () ->
        let request : Backend.terminate_request =
          {
            workflow_id = handle.workflow_id;
            run_id = handle.run_id;
            reason;
          }
        in
        Backend.client_terminate handle.client.backend request

(** Allocates a fresh request ID for a signal when the caller did not supply
    one. Unlike cancellation, separate signal calls are distinct messages by
    default, even when they target the same run and signal name. Supplying an
    explicit ID gives a caller retry-safe idempotency semantics. *)
let generated_signal_request_id () =
  let sequence = Atomic.fetch_and_add next_signal_request_id 1 in
  Printf.sprintf "ocaml-client-signal-%d" sequence

(** Sends one typed signal to the exact run retained by [handle]. The input is
    encoded before the backend call, and success means only that Temporal
    acknowledged the RPC; workflow code may process it asynchronously. *)
let signal ?request_id handle ~(signal : 'signal Signal.t) ~input =
  if Atomic.get handle.client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    match validate_signal_fields ~request_id with
    | Error error -> Error error
    | Ok () -> (
        match Codec.encode (Signal.input signal) input with
        | Error error -> Error error
        | Ok encoded_input ->
            let request_id =
              match request_id with
              | Some request_id -> request_id
              | None -> generated_signal_request_id ()
            in
            let request : Backend.signal_request =
              {
                workflow_id = handle.workflow_id;
                run_id = handle.run_id;
                signal_name = Signal.name signal;
                request_id;
                input = encoded_input;
              }
            in
            Backend.client_signal handle.client.backend request)

(** Executes one output-only query against the exact run retained by [handle].
    Query arguments are intentionally absent in this first client slice: the
    workflow-side [Query] definition is already output-only, and the result is
    decoded with the definition's codec only after the native bridge has
    validated the response payload. *)
let query handle ~(query : 'query Query.t) =
  if Atomic.get handle.client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    let request : Backend.query_request =
      {
        workflow_id = handle.workflow_id;
        run_id = handle.run_id;
        query_name = Query.name query;
      }
    in
    Result.bind (Backend.client_query handle.client.backend request) (fun payload ->
        Codec.decode (Query.output query) payload)

(** Lists one bounded visibility page through the client's backend. The public
    layer validates caller-controlled query metadata before the request enters
    either the deterministic mock or the native supervisor. *)
let list_visibility ?(page_size = 100) ?page_token client ~query () =
  if Atomic.get client.closed then
    Error (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else if page_size < 1 || page_size > 1_000 then
    Error
      (Error.defect
         ~message:"visibility page_size must be between 1 and 1000")
  else
    if String.length query > 65_536 then
      Error
        (Error.defect
           ~message:"visibility query exceeds the protocol safety limit")
    else if String.contains query '\000' then
      Error (Error.defect ~message:"visibility query must not contain NUL")
    else (
        match page_token with
        | Some token when String.equal token "" ->
            Error
              (Error.defect ~message:"visibility page token must not be empty")
        | Some token -> (
            match validate_name "visibility page token" token with
            | Error error -> Error error
            | Ok () ->
                let request : Backend.visibility_request =
                  { query; page_size; next_page_token = Some token }
                in
                Result.map
                  (fun (page : Backend.visibility_page) ->
                    {
                      executions =
                        List.map
                          (fun (execution : Backend.visibility_execution) ->
                            {
                              workflow_id = execution.workflow_id;
                              run_id = execution.run_id;
                              workflow_type = execution.workflow_type;
                              task_queue = execution.task_queue;
                              status = execution.status;
                            })
                          page.executions;
                      next_page_token = page.next_page_token;
                    })
                  (Backend.client_list_visibility client.backend request))
        | None ->
            let request : Backend.visibility_request =
              { query; page_size; next_page_token = None }
            in
            Result.map
              (fun (page : Backend.visibility_page) ->
                {
                  executions =
                    List.map
                      (fun (execution : Backend.visibility_execution) ->
                        {
                          workflow_id = execution.workflow_id;
                          run_id = execution.run_id;
                          workflow_type = execution.workflow_type;
                          task_queue = execution.task_queue;
                          status = execution.status;
                        })
                      page.executions;
                  next_page_token = page.next_page_token;
                })
              (Backend.client_list_visibility client.backend request))

(** Returns the durable workflow identity retained by a handle. *)
let workflow_id (handle : ('input, 'output) handle) = handle.workflow_id

(** Returns the exact server run identity retained by a handle. *)
let run_id (handle : ('input, 'output) handle) = handle.run_id

(** Closes backend resources once and returns the same cached result to later
    shutdown callers.
    Native supervisor shutdown is terminal and cached: even when its result is
    an error, the backend contract says the complete graph was consumed or
    invalidated, so the atomic state transition cannot hide a live resource. *)
let shutdown client =
  Mutex.lock client.shutdown_mutex;
  (* [Fun.protect] is the single release path so a concurrent caller cannot be
     left waiting if backend teardown raises before it can return a result. *)
  Fun.protect
    ~finally:(fun () -> Mutex.unlock client.shutdown_mutex)
    (fun () ->
      match client.shutdown_result with
      | Some result -> result
      | None ->
          (* Close admission before entering native teardown. Concurrent starts
             that already passed their check are ordered by the supervisor; later
             callers observe the closed bit and cannot enqueue new work. *)
          Atomic.set client.closed true;
          let result =
            try Backend.client_shutdown client.backend with
            | exception_ ->
                Error
                  (Error.defect
                     ~message:
                       (Printf.sprintf "client shutdown raised: %s"
                          (Printexc.to_string exception_)))
          in
          client.shutdown_result <- Some result;
          result)
