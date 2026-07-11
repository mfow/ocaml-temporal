type status =
  | Invalid_argument
  | Abi_mismatch
  | Panic
  | Internal
  | Unknown of int

type error = {
  status : status;
  message : string;
}

val abi_version : int32
val check_abi_version : int32 -> (unit, error) result
val echo : bytes -> (bytes, error) result
val conformance_wait_ms : int -> (unit, error) result
