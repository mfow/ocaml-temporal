(** A bounded, Domain-safe FIFO processor for typed request languages.

    This library is private to the repository. Its blocking operations are
    intended for ordinary OCaml Domains, not cooperative scheduler Domains.
    A fiber runtime must offload [post], [call], and [join] to a blocking
    bridge rather than invoking them on its scheduler Domain. *)

(** A typed request language. The type parameter is the result produced by
    handling a request. Request languages can encode expected operational
    failures directly, for example as [('value, 'error) result t]. *)
module type Request = sig
  type _ t
end

(** Builds an isolated processor implementation for one typed request
    language. *)
module Make (Request : Request) : sig
  (** A terminal reason returned by admission, calls, and joins. *)
  type failure =
    | Closed
        (** The processor is closing or has completed an orderly shutdown. *)
    | Handler_raised of exn
        (** The owner Domain contained an unexpected handler exception. *)

  (** A rank-2 handler. Every invocation occurs sequentially on the one owner
      Domain, so mutable state captured by this closure can remain owner-only.

      A handler must not call [post], [call], or [join] on that same processor.
      [call] and [join] cannot complete while the handler occupies the sole
      owner Domain, and [post] can block forever when the bounded FIFO is full.
      Calling [close] from the handler is safe: it rejects later admissions and
      the owner drains requests already admitted after the handler returns. *)
  type handler = { handle : 'result. 'result Request.t -> 'result }

  (** An abstract processor handle. Synchronization primitives, reply cells,
      queue representation, and the owner Domain are deliberately hidden. *)
  type t

  (** A typed one-shot result for a request already admitted to the FIFO. The
      representation and settlement operation remain owner-private. *)
  type 'result pending

  (** [create ~capacity ~handler] starts one owner Domain. [capacity] is the
      maximum number of admitted requests waiting in the FIFO, excluding the
      request currently executing. A non-positive capacity raises
      [Invalid_argument]. *)
  val create : capacity:int -> handler:handler -> t

  (** [post processor request] admits a fire-and-forget request. It blocks an
      ordinary producer Domain while the FIFO is full and returns after
      admission, not after handling. Therefore a later handler failure cannot
      be reported retroactively to an already successful post. *)
  val post : t -> unit Request.t -> (unit, failure) result

  (** [call processor request] admits [request] with the same bounded
      backpressure as [post], then blocks until the owner returns its typed
      result or the processor fails. *)
  val call : t -> 'result Request.t -> ('result, failure) result

  (** [submit_and_close processor request] atomically admits one final typed
      request at the FIFO tail and changes the lifecycle from open to closing.
      It never waits for ordinary queue capacity. Returning [Ok pending]
      proves that the close transition has linearized; the request itself may
      still be waiting behind earlier admitted work. *)
  val submit_and_close :
    t -> 'result Request.t -> ('result pending, failure) result

  (** [await pending] blocks until the owner settles the already admitted
      request or reports its terminal handler failure. *)
  val await : 'result pending -> ('result, failure) result

  (** [call_and_close processor request] atomically admits one final typed
      request at the FIFO tail and changes the lifecycle from open to closing.
      It never waits for ordinary queue capacity: the terminal request uses
      one reserved slot beyond [capacity], so shutdown cannot be starved by
      producers already waiting for capacity. Later admissions are rejected.
      It is equivalent to [submit_and_close] followed by [await]. If the
      processor was already closing or terminal, no request is admitted. *)
  val call_and_close : t -> 'result Request.t -> ('result, failure) result

  (** [close processor] is idempotent. It rejects new admissions and asks the
      owner to drain every request admitted before close linearized. *)
  val close : t -> unit

  (** [join processor] blocks until the owner Domain terminates and is safe to
      call repeatedly. It returns the handler exception that caused terminal
      failure, if any. Call [close] first for orderly shutdown. *)
  val join : t -> (unit, failure) result
end
