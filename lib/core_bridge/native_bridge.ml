(** OCaml names for Rust bridge status codes. An unrecognized number is kept in
    [Unknown] so diagnostics remain available across a version mismatch. *)
type status =
  | Invalid_argument
  | Abi_mismatch
  | Panic
  | Internal
  | Unknown of int

(** Error data copied into OCaml. It never owns Rust memory. *)
type error = {
  status : status;
  message : string;
}

(** Version requested by this binding layer. *)
let abi_version = 1l

(** Private OCaml value implemented in C. It owns a Rust result allocation until
    [decode] frees it or the OCaml garbage collector runs its finalizer. *)
type response

(** Opaque owner of one Temporal Core runtime and its Tokio executor. Only the
    SDK supervisor may use or close it; workflow code never sees this type. *)
type runtime

external check_abi_version_raw : int32 -> response
  = "ocaml_temporal_check_abi_version"

external echo_raw : bytes -> response = "ocaml_temporal_echo"

external conformance_wait_ms_raw : int -> response
  = "ocaml_temporal_conformance_wait_ms"

external response_status : response -> int = "ocaml_temporal_response_status"
external response_value : response -> bytes = "ocaml_temporal_response_value"
external response_error : response -> string = "ocaml_temporal_response_error"
external response_free : response -> unit = "ocaml_temporal_response_free"
external runtime_create_raw : unit -> runtime * response
  = "ocaml_temporal_runtime_create"

external runtime_close_raw : runtime -> int = "ocaml_temporal_runtime_close"

(** Converts known numeric statuses and retains every newer value as [Unknown]. *)
let status = function
  | 1 -> Invalid_argument
  | 2 -> Abi_mismatch
  | 3 -> Panic
  | 4 -> Internal
  | code -> Unknown code

(** Copies either the successful bytes or error message into OCaml, then always
    frees the Rust allocation. [Fun.protect] still runs cleanup if copying
    raises an OCaml exception. *)
let decode response =
  Fun.protect
    ~finally:(fun () -> response_free response)
    (fun () ->
      let code = response_status response in
      if code = 0 then Ok (response_value response)
      else
        Error
          { status = status code; message = response_error response })

(** Converts successful test operations with no useful output to [Ok ()] after
    [decode] has performed the normal memory cleanup. *)
let check_abi_version version =
  Result.map (fun _ -> ()) (decode (check_abi_version_raw version))

let echo input = decode (echo_raw input)

let conformance_wait_ms milliseconds =
  Result.map (fun _ -> ())
    (decode (conformance_wait_ms_raw milliseconds))

(** Closes the native owner after first clearing its OCaml-held pointer. This
    makes repeated sequential calls safe; the future supervisor actor will
    serialize all lifecycle calls across Domains. *)
let runtime_close runtime =
  match runtime_close_raw runtime with
  | 0 -> Ok ()
  | code ->
      Error
        { status = status code; message = "Temporal Core runtime close failed" }

(** Checks the linked bridge contract once, then creates the native runtime.
    If creation fails after allocating the OCaml owner, cleanup remains safe
    because its native pointer is either null or explicitly closed here. *)
let runtime_create () =
  match check_abi_version abi_version with
  | Error _ as error -> error
  | Ok () ->
      let runtime, response = runtime_create_raw () in
      (match decode response with
      | Ok _ -> Ok runtime
      | Error error ->
          ignore (runtime_close runtime);
          Error error)
