(** Starts [definition] as a durable child workflow and returns immediately.
    [id] is the child execution's application-owned durable identity. It must
    be non-empty, valid UTF-8, and no more than 65,536 UTF-8 bytes so it can
    cross the strict bridge boundary. Invalid identity or input encoding returns
    a ready failed future without emitting a command or consuming a sequence.
    Starting several operations before awaiting them lets Temporal run them
    concurrently. *)
val start :
  id:string ->
  ('input, 'output) Workflow.t ->
  'input ->
  ('output, Error.t) Future.t

(** Starts a child workflow and waits for its result. This is equivalent to
    [Future.await (start ~id definition input)]. Child, codec, cancellation,
    and bridge failures are returned as structured values. *)
val execute :
  id:string ->
  ('input, 'output) Workflow.t ->
  'input ->
  ('output, Error.t) result
