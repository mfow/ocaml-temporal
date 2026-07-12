(** Converts public payloads at the private base/protocol boundary. Every
    conversion copies mutable bytes so a caller cannot mutate data retained by
    the Rust bridge or by a worker's activation state. *)

(* Copies a public payload into the private base representation. *)
(** Copies public metadata and bytes into the base payload representation so a
    private protocol adapter never retains mutable application storage. *)
let to_base (payload : Payload.t) : Temporal_base.Payload.t =
  {
    Temporal_base.Payload.metadata = List.map (fun (key, value) -> (key, value)) payload.metadata;
    data = Bytes.copy payload.data;
  }

(* Copies a base payload into the public representation. *)
(** Copies a base payload back into an owned public record at the adapter
    boundary, preventing private payload layout from entering public results. *)
let of_base (payload : Temporal_base.Payload.t) : Payload.t =
  {
    Payload.metadata = List.map (fun (key, value) -> (key, value)) payload.metadata;
    data = Bytes.copy payload.data;
  }
