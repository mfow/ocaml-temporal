(** Typed workflow query definitions.

    A query reads workflow state and returns a value without changing durable
    history. Query handlers are required by contract to be read-only and
    non-suspending. The deterministic dispatcher invokes them outside an active
    workflow scheduler, so accidental use of a scheduler-owned operation
    returns a typed lifecycle error rather than blocking a thread. *)

(** A validated query name paired with the type of its result. Queries have no
    input payload in this API; use a record or tuple codec when a query needs
    parameters. *)
type 'output definition

(** Public name for a query definition. *)
type 'output t = 'output definition

(** Creates a query definition after validating its stable name. *)
val define : name:string -> output:'output Codec.t -> 'output t

(** Returns the stable name used to route a query request. *)
val name : 'output t -> string

(** Returns the codec used to encode and decode query results. *)
val output : 'output t -> 'output Codec.t

(** A query handler closes over read-only workflow state and computes one typed
    result. The callback must not mutate workflow state, schedule commands, or
    wait for a future. *)
module Handler : sig
  (** An existentially packaged query handler. *)
  type t

  (** Builds a read-only handler for [query]. *)
  val make : 'output definition -> (unit -> ('output, Error.t) result) -> t

  (** Convenience alias for [make]. *)
  val handle : 'output definition -> (unit -> ('output, Error.t) result) -> t

  (** Returns the query name used to index an interaction registry. *)
  val name : t -> string

  (** Invokes a query and encodes its typed result. *)
  val dispatch : t -> (Payload.t, Error.t) result
end
