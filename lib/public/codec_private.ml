(** Adapts an opaque public codec to the base codec record required by the
    worker/runtime libraries. The base constructor accepts complete payload
    callbacks, so this preserves metadata-sensitive codecs such as [option]
    without exposing their representation in the installed API. *)

(* Converts a complete public payload codec to the base codec representation
   without assuming that one value type has one fixed encoding metadata value. *)
(** Rebuilds the public codec as the base record expected by command adapters;
    the public codec remains the source of truth for application callbacks. *)
let to_base (codec : 'a Codec.t) : 'a Temporal_base.Codec.t =
  Temporal_base.Codec.of_payload
    ~encode:(fun value ->
      match Codec.encode codec value with
      | Ok payload -> Ok (Payload_private.to_base payload)
      | Error error -> Error (Error_private.to_base error))
    ~decode:(fun payload ->
      match Codec.decode codec (Payload_private.of_base payload) with
      | Ok value -> Ok value
      | Error error -> Error (Error_private.to_base error))

(* Encodes through the public callbacks and copies the resulting payload into
   the representation accepted by private runtime commands. *)
(** Encodes through the public codec and translates both payload and error at
    the private boundary before a protocol command can be emitted. *)
let encode_base (codec : 'a Codec.t) value =
  match Codec.encode codec value with
  | Ok payload -> Ok (Payload_private.to_base payload)
  | Error error -> Error (Error_private.to_base error)

(* Decodes a private payload through public callbacks, copying bytes before
   invoking code owned by the application. *)
(** Decodes a base payload through the public codec while preserving the typed
    public error vocabulary for callers of the adapter. *)
let decode_base (codec : 'a Codec.t) payload =
  match Codec.decode codec (Payload_private.of_base payload) with
  | Ok value -> Ok value
  | Error error -> Error (Error_private.to_base error)
