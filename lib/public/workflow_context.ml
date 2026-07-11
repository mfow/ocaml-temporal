let is_active () =
  Option.is_some (Temporal_runtime.Workflow_context_store.current ())
