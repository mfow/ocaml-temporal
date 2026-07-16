(* This module is a compile-time compatibility witness for the installed
   package.  The annotations deliberately spell out the supported types at
   the consumer boundary instead of inferring them from [Temporal] itself.
   A removed value, changed label, changed result type, or accidentally hidden
   module therefore fails the installed-consumer build before a release can be
   published.  The witness has no side effects and is not a runtime test. *)

module T = Temporal

(* The root module is the public allow-list.  These aliases make every intended
   module name part of the consumer compilation while private implementation
   modules remain absent from the fixture's include path. *)
module Activity = T.Activity
module Child_workflow = T.Child_workflow
module Client = T.Client
module Codec = T.Codec
module Condition = T.Condition
module Duration = T.Duration
module Error = T.Error
module Future = T.Future
module Interaction = T.Interaction
module Payload = T.Payload
module Query = T.Query
module Result_syntax = T.Result_syntax
module Runtime_info = T.Runtime_info
module Scope = T.Scope
module Signal = T.Signal
module Time = T.Time
module Update = T.Update
module Worker = T.Worker
module Workflow = T.Workflow
module Workflow_context = T.Workflow_context

(* Core value definitions and their accessors are the stable authoring
   boundary.  Keep the type variables explicit: an annotation that silently
   becomes monomorphic would make this witness weaker than a real consumer. *)
let _activity_define :
    name:string ->
    input:'input T.Codec.t ->
    output:'output T.Codec.t ->
    ('input -> ('output, T.Error.t) result) ->
    ('input, 'output) T.Activity.t =
  T.Activity.define

let _activity_define_with_context :
    name:string ->
    input:'input T.Codec.t ->
    output:'output T.Codec.t ->
    (T.Activity.context ->
     'input ->
     ('output, T.Error.t) result) ->
    ('input, 'output) T.Activity.t =
  T.Activity.define_with_context

let _activity_remote :
    name:string ->
    input:'input T.Codec.t ->
    output:'output T.Codec.t ->
    ('input, 'output) T.Activity.t =
  T.Activity.remote

let _activity_name : ('input, 'output) T.Activity.t -> string = T.Activity.name

let _activity_input :
    ('input, 'output) T.Activity.t -> 'input T.Codec.t =
  T.Activity.input

let _activity_output :
    ('input, 'output) T.Activity.t -> 'output T.Codec.t =
  T.Activity.output

let _activity_execute :
    ?activity_id:string ->
    ?task_queue:string ->
    ?schedule_to_close_timeout:T.Duration.t ->
    ?schedule_to_start_timeout:T.Duration.t ->
    ?start_to_close_timeout:T.Duration.t ->
    ?heartbeat_timeout:T.Duration.t ->
    ?retry_policy:T.Activity.Retry_policy.t ->
    ?cancellation_type:T.Activity.cancellation_type ->
    ?do_not_eagerly_execute:bool ->
    ('input, 'output) T.Activity.t ->
    'input ->
    ('output, T.Error.t) result =
  T.Activity.execute

let _activity_future :
    'output T.Activity.handle -> ('output, T.Error.t) T.Future.t =
  T.Activity.future

let _activity_cancel :
    'output T.Activity.handle -> (unit, T.Error.t) result =
  T.Activity.cancel

let _activity_heartbeat :
    T.Activity.context -> 'a T.Codec.t -> 'a -> (unit, T.Error.t) result =
  T.Activity.heartbeat

let _activity_retry_policy_make :
    initial_interval:T.Duration.t ->
    backoff_coefficient:float ->
    maximum_interval:T.Duration.t ->
    maximum_attempts:int ->
    ?non_retryable_error_types:string list ->
    unit -> (T.Activity.Retry_policy.t, T.Error.t) result =
  T.Activity.Retry_policy.make

let _child_start :
    ?cancellation_type:T.Child_workflow.cancellation_type ->
    ?retry_policy:T.Activity.Retry_policy.t ->
    id:string ->
    ('input, 'output) T.Workflow.t ->
    'input ->
    ('output, T.Error.t) T.Future.t =
  T.Child_workflow.start

let _child_execute :
    ?cancellation_type:T.Child_workflow.cancellation_type ->
    ?retry_policy:T.Activity.Retry_policy.t ->
    id:string ->
    ('input, 'output) T.Workflow.t ->
    'input ->
    ('output, T.Error.t) result =
  T.Child_workflow.execute

(* Codec construction and the built-in codecs define the payload boundary
   shared by workflows, activities, and the client. *)
let _codec_make :
    encoding:string ->
    encode:('a -> (bytes, T.Error.t) result) ->
    decode:(bytes -> ('a, T.Error.t) result) ->
    'a T.Codec.t =
  T.Codec.make

let _codec_encode :
    'a T.Codec.t -> 'a -> (T.Codec.payload, T.Error.t) result =
  T.Codec.encode

let _codec_decode :
    'a T.Codec.t -> T.Codec.payload -> ('a, T.Error.t) result =
  T.Codec.decode

let _codec_option : 'a T.Codec.t -> 'a option T.Codec.t = T.Codec.option

let _duration_of_ms : int64 -> T.Duration.t = T.Duration.of_ms
let _duration_to_ms : T.Duration.t -> int64 = T.Duration.to_ms

let _error_make :
    ?non_retryable:bool ->
    ?details:T.Payload.t list ->
    category:T.Error.category ->
    message:string ->
    unit -> T.Error.t =
  T.Error.make

let _error_view : T.Error.t -> T.Error.view = T.Error.view
let _error_kind : T.Error.t -> string = T.Error.kind
let _error_message : T.Error.t -> string = T.Error.message

(* The future combinators are intentionally checked separately from workflow
   command starters: they are the public direct-style composition vocabulary. *)
let _future_await :
    ('value, 'error) T.Future.t -> ('value, 'error) result =
  T.Future.await

let _future_map :
    ('value -> 'mapped) ->
    ('value, 'error) T.Future.t ->
    ('mapped, 'error) T.Future.t =
  T.Future.map

let _future_both :
    ('left, T.Error.t) T.Future.t ->
    ('right, T.Error.t) T.Future.t ->
    ('left * 'right, T.Error.t) T.Future.t =
  T.Future.both

let _future_all :
    ('value, T.Error.t) T.Future.t list ->
    ('value list, T.Error.t) T.Future.t =
  T.Future.all

let _future_race :
    ('left, T.Error.t) T.Future.t ->
    ('right, T.Error.t) T.Future.t ->
    (('left, 'right) T.Future.race, T.Error.t) T.Future.t =
  T.Future.race

let _future_is_ready : ('value, 'error) T.Future.t -> bool = T.Future.is_ready
let _future_peek :
    ('value, 'error) T.Future.t -> ('value, 'error) result option =
  T.Future.peek

(* Workflow and interaction definitions are ordinary typed values.  Their
   annotations protect the direct-style authoring API and handler registration
   types without attempting to execute a workflow in this consumer fixture. *)
let _workflow_define :
    name:string ->
    input:'input T.Codec.t ->
    output:'output T.Codec.t ->
    ('input -> ('output, T.Error.t) result) ->
    ('input, 'output) T.Workflow.t =
  T.Workflow.define

let _workflow_remote :
    name:string ->
    input:'input T.Codec.t ->
    output:'output T.Codec.t ->
    ('input, 'output) T.Workflow.t =
  T.Workflow.remote

let _workflow_start_sleep :
    T.Duration.t -> (unit, T.Error.t) T.Future.t =
  T.Workflow.start_sleep

let _workflow_sleep : T.Duration.t -> (unit, T.Error.t) result = T.Workflow.sleep
let _workflow_now : unit -> (T.Time.t, T.Error.t) result = T.Workflow.now
let _workflow_patched : id:string -> bool = T.Workflow.patched
let _workflow_deprecate_patch : id:string -> unit = T.Workflow.deprecate_patch

let _signal_define :
    name:string -> input:'input T.Codec.t -> 'input T.Signal.t =
  T.Signal.define

let _signal_handler_make :
    'input T.Signal.t ->
    ('input -> (unit, T.Error.t) result) ->
    T.Signal.Handler.t =
  T.Signal.Handler.make

let _query_define :
    name:string -> output:'output T.Codec.t -> 'output T.Query.t =
  T.Query.define

let _query_handler_make :
    'output T.Query.t ->
    (unit -> ('output, T.Error.t) result) ->
    T.Query.Handler.t =
  T.Query.Handler.make

let _update_define :
    name:string ->
    input:'input T.Codec.t ->
    output:'output T.Codec.t ->
    ('input, 'output) T.Update.t =
  T.Update.define

let _update_handler_make :
    ?validator:('input -> (unit, T.Error.t) result) ->
    ('input, 'output) T.Update.t ->
    ('input -> ('output, T.Error.t) result) ->
    T.Update.Handler.t =
  T.Update.Handler.make

let _interaction_create :
    ?signals:T.Signal.Handler.t list ->
    ?queries:T.Query.Handler.t list ->
    ?updates:T.Update.Handler.t list ->
    unit -> (T.Interaction.t, T.Error.t) result =
  T.Interaction.create

let _interaction_signal :
    T.Interaction.t -> 'input T.Signal.t -> 'input -> (unit, T.Error.t) result =
  T.Interaction.signal

let _interaction_query :
    T.Interaction.t -> 'output T.Query.t -> ('output, T.Error.t) result =
  T.Interaction.query

let _interaction_update :
    T.Interaction.t ->
    ('input, 'output) T.Update.t ->
    'input -> ('output, T.Error.t) result =
  T.Interaction.update

(* Client and worker annotations make the process lifecycle contract explicit;
   these are the operations an installed consumer must be able to compose. *)
let _client_create :
    ?identity:string ->
    target_url:string ->
    namespace:string ->
    unit -> (T.Client.t, T.Error.t) result =
  T.Client.create

let _client_start :
    T.Client.t ->
    ?request_id:string ->
    workflow:('input, 'output) T.Workflow.t ->
    task_queue:string ->
    id:string ->
    input:'input ->
    unit -> (('input, 'output) T.Client.handle, T.Error.t) result =
  T.Client.start

let _client_follow :
    T.Client.t ->
    workflow:('input, 'output) T.Workflow.t ->
    T.Client.execution ->
    (('input, 'output) T.Client.handle, T.Error.t) result =
  T.Client.follow

let _client_wait :
    ('input, 'output) T.Client.handle ->
    ('output T.Client.terminal_result, T.Error.t) result =
  T.Client.wait

let _client_cancel :
    ?request_id:string ->
    ?reason:string ->
    ('input, 'output) T.Client.handle ->
    (unit, T.Error.t) result =
  T.Client.cancel

let _client_signal :
    ?request_id:string ->
    ('workflow_input, 'workflow_output) T.Client.handle ->
    signal:'signal T.Signal.t ->
    input:'signal ->
    (unit, T.Error.t) result =
  T.Client.signal

let _client_query :
    ('workflow_input, 'workflow_output) T.Client.handle ->
    query:'query T.Query.t ->
    ('query, T.Error.t) result =
  T.Client.query

let _client_workflow_id :
    ('input, 'output) T.Client.handle -> string =
  T.Client.workflow_id

let _client_run_id : ('input, 'output) T.Client.handle -> string = T.Client.run_id
let _client_shutdown : T.Client.t -> (unit, T.Error.t) result = T.Client.shutdown

let _worker_workflow :
    ?signals:T.Signal.Handler.t list ->
    ?queries:T.Query.Handler.t list ->
    ?updates:T.Update.Handler.t list ->
    ('input, 'output) T.Workflow.t -> T.Worker.registered_workflow =
  T.Worker.workflow

let _worker_activity :
    ('input, 'output) T.Activity.t -> T.Worker.registered_activity =
  T.Worker.activity

let _worker_create :
    ?identity:string ->
    ?max_cached_workflows:int ->
    target_url:string ->
    namespace:string ->
    task_queue:string ->
    workflows:T.Worker.registered_workflow list ->
    activities:T.Worker.registered_activity list ->
    unit -> (T.Worker.t, T.Error.t) result =
  T.Worker.create

let _worker_run : T.Worker.t -> (unit, T.Error.t) result = T.Worker.run
let _worker_shutdown : T.Worker.t -> (unit, T.Error.t) result = T.Worker.shutdown

(* The remaining small modules still participate in the public contract. *)
let _condition_wait_until : (unit -> bool) -> (unit, T.Error.t) result =
  T.Condition.wait_until

let _condition_wait_until_result :
    T.Condition.predicate -> (unit, T.Error.t) result =
  T.Condition.wait_until_result

let _runtime_abi_version : unit -> (int32, T.Error.t) result =
  T.Runtime_info.native_bridge_abi_version

let _scope_create : unit -> (T.Scope.t, T.Error.t) result = T.Scope.create
let _scope_cancel : T.Scope.t -> (unit, T.Error.t) result = T.Scope.cancel
let _scope_check : T.Scope.t -> (unit, T.Error.t) result = T.Scope.check

let _time_of_unix :
    seconds:int64 -> nanoseconds:int -> (T.Time.t, T.Error.t) result =
  T.Time.of_unix

let _time_compare : T.Time.t -> T.Time.t -> int = T.Time.compare
let _workflow_context_active : unit -> bool = T.Workflow_context.is_active

(* Keep the public payload record and result syntax visible to a real consumer.
   These expressions are never evaluated by the test executable; they only
   force the installed CMI to expose the documented record fields and operator
   types. *)
let _payload_fields (payload : T.Payload.t) : (string * string) list * bytes =
  (payload.metadata, payload.data)

let _result_bind :
    ('a, 'error) result ->
    ('a -> ('b, 'error) result) ->
    ('b, 'error) result =
  T.Result_syntax.( let* )

let _result_map : ('a, 'error) result -> ('a -> 'b) -> ('b, 'error) result =
  T.Result_syntax.( let+ )
