(* The explicit root module is the package's allow-list.  Dune normally
   generates aliases for every implementation file in a wrapped library,
   which would make private adapters reachable as [Temporal.Backend] or
   [Temporal.Native_worker].  Keeping the root module in source lets us expose
   only the supported public modules while the implementation files remain
   available to one another inside this library. *)
module Activity = Activity
module Child_workflow = Child_workflow
module Client = Client
module Codec = Codec
module Duration = Duration
module Error = Error
module Future = Future
module Interaction = Interaction
module Payload = Payload
module Query = Query
module Result_syntax = Result_syntax
module Runtime_info = Runtime_info
module Scope = Scope
module Time = Time
module Signal = Signal
module Update = Update
module Worker = Worker
module Workflow = Workflow
module Workflow_context = Workflow_context
