type ('value, 'error) t = ('value, 'error) Temporal_runtime.Future_store.t

let await = Temporal_runtime.Future_store.await
let map = Temporal_runtime.Future_store.map
let map_error = Temporal_runtime.Future_store.map_error
let both = Temporal_runtime.Future_store.both
let is_ready = Temporal_runtime.Future_store.is_ready
let peek = Temporal_runtime.Future_store.peek
