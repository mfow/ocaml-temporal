(** Public future handles are views over one workflow scheduler.  Their
    callback representation lives in [Temporal_future_kernel], a
    package-private library;
    applications can only use the operations declared by this interface and
    cannot fabricate scheduler callbacks or retain runtime handles. *)
type ('value, 'error) t = ('value, 'error) Temporal_future_kernel.t

type ('left, 'right) race = Left of 'left | Right of 'right

(** Builds a private future handle.  Only package-private adapters call this;
    keeping the function out of [future.mli] prevents arbitrary construction
    outside the SDK's deterministic lifecycle. *)
let make_repr ~await ~await_gate ~observe ~is_ready ~peek ~owner_id
    ~outside_error ~callbacks_live ~enqueue =
  Temporal_future_kernel.make ~await ~await_gate ~observe ~is_ready ~peek ~owner_id
    ~outside_error ~callbacks_live ~enqueue

(** Converts one internal runtime future to the public error vocabulary. This
    helper is used only for the context-owned empty aggregate; normal command
    futures enter through [Future_private.of_internal]. *)
let of_internal ~outside_error future =
  let map_error result = Result.map_error Error_private.of_base result in
  make_repr
    ~await:(fun () -> map_error (Temporal_runtime.Future_store.await future))
    ~await_gate:(fun register ->
      Temporal_runtime.Future_store.await_gate future register)
    ~observe:(fun observer ->
      Temporal_runtime.Future_store.observe future (fun result ->
          observer (map_error result)))
    ~is_ready:(fun () -> Temporal_runtime.Future_store.is_ready future)
    ~peek:(fun () -> Option.map map_error (Temporal_runtime.Future_store.peek future))
    ~owner_id:(Temporal_runtime.Future_store.owner_id future)
    ~outside_error
    ~callbacks_live:(fun () -> Temporal_runtime.Future_store.callbacks_live future)
    ~enqueue:(Temporal_runtime.Future_store.enqueue future)

(** Returns the result, suspending the current workflow fiber only when the
    owning scheduler can resume it. *)
let await future = Temporal_future_kernel.await future

(** Reports readiness without scheduling or waiting. *)
let is_ready future = Temporal_future_kernel.is_ready future

(** Inspects a ready result without consuming it. *)
let peek future = Temporal_future_kernel.peek future

(** Creates a future that shares [parent]'s scheduler and receives observer
    notifications in scheduler order. Derived futures do not add a second
    pending counter; the source operations already keep the workflow blocked. *)
let make_derived ~parent ~outside_error =
  let result = ref None in
  let observers = ref [] in
  let resolve value =
    match !result with
    | Some _ -> invalid_arg "Temporal future resolved more than once"
    | None ->
        result := Some value;
        let callbacks = List.rev !observers in
        observers := [];
        List.iter
          (fun callback ->
            Temporal_future_kernel.enqueue parent (fun () ->
                if Temporal_future_kernel.callbacks_live parent then
                  callback value))
          callbacks
  in
  let observe callback =
    match !result with
    | Some value ->
        Temporal_future_kernel.enqueue parent (fun () ->
            if Temporal_future_kernel.callbacks_live parent then
              callback value)
    | None -> observers := callback :: !observers
  in
  let await_gate register = Temporal_future_kernel.await_gate parent register in
  let await () =
    match !result with
    | Some value -> value
    | None ->
        (* Outside the owning scheduler fiber, return a pure defect without
           allocating a gate future on the parent scheduler (which would race
           another Domain or leak after shutdown). *)
        if not
             (Temporal_runtime.Future_store.current_owner_matches
                (Temporal_future_kernel.owner_id parent))
             || not (Temporal_future_kernel.callbacks_live parent)
        then Error (outside_error ())
        else
          let observed = ref None in
          await_gate (fun signal ->
              observe (fun value ->
                  if Option.is_none !observed then (
                    observed := Some value;
                    signal ())));
          Option.value !observed ~default:(Error (outside_error ()))
  in
  let future =
    make_repr ~await ~await_gate ~observe
      ~is_ready:(fun () -> Option.is_some !result)
      ~peek:(fun () -> !result)
      ~owner_id:(Temporal_future_kernel.owner_id parent) ~outside_error
      ~callbacks_live:(fun () -> Temporal_future_kernel.callbacks_live parent)
      ~enqueue:(Temporal_future_kernel.enqueue parent)
  in
  (future, resolve)

(** A ready value can retain its source owner so a later combinator still
    rejects accidental cross-workflow composition deterministically. The gate
    delegates to [source]'s own gate (rather than a no-op) so a still-pending
    [make_derived] future built on top of this one suspends on the real
    owning scheduler instead of falling through to a synchronous
    [outside_error]; [source]'s gate builds a fresh suspension point from its
    real owner regardless of [source]'s own readiness, so this is safe even
    though [source] itself is already settled. *)
let ready_like source result =
  make_repr ~await:(fun () -> result)
    ~await_gate:(fun register -> Temporal_future_kernel.await_gate source register)
    ~observe:(fun callback ->
      Temporal_future_kernel.enqueue source (fun () ->
          if Temporal_future_kernel.callbacks_live source then callback result))
    ~is_ready:(fun () -> true) ~peek:(fun () -> Some result)
    ~owner_id:(Temporal_future_kernel.owner_id source)
    ~outside_error:(Temporal_future_kernel.outside_error source)
    ~callbacks_live:(fun () -> Temporal_future_kernel.callbacks_live source)
    ~enqueue:(Temporal_future_kernel.enqueue source)

(** Builds the structured defect shared by aggregate ownership checks. *)
let ownership_error () =
  Error.defect
    ~message:
      "Temporal future combinator received futures from different workflow \
       executions"

(** Checks scheduler identity before registering any observers, so an invalid
    cross-workflow aggregate fails as a value without mutating either owner. *)
let same_owner first rest =
  List.for_all
    (fun future ->
      Temporal_future_kernel.owner_id future
      = Temporal_future_kernel.owner_id first)
    rest

(** Maps a successful result without waiting for it at construction time. *)
let map mapper source =
  let mapped, resolve =
    make_derived ~parent:source
      ~outside_error:(Temporal_future_kernel.outside_error source)
  in
  Temporal_future_kernel.observe source (fun result ->
      resolve (Result.map mapper result));
  mapped

(** Maps both stored errors and errors returned outside the owner scheduler. *)
let map_error mapper source =
  let mapped, resolve =
    make_derived ~parent:source
      ~outside_error:(fun () ->
        mapper ((Temporal_future_kernel.outside_error source) ()))
  in
  Temporal_future_kernel.observe source (fun result ->
      resolve (Result.map_error mapper result));
  mapped

(** Waits for two same-owner futures, retaining the left error when both fail. *)
let both left right =
  if Temporal_future_kernel.owner_id left
     <> Temporal_future_kernel.owner_id right then
    ready_like left (Error (ownership_error ()))
  else
    let combined, resolve =
      make_derived ~parent:left
        ~outside_error:(Temporal_future_kernel.outside_error left)
    in
    let left_result = ref None in
    let right_result = ref None in
    let finish () =
      match (!left_result, !right_result) with
      | Some (Ok left), Some (Ok right) -> resolve (Ok (left, right))
      | Some (Error error), Some _ -> resolve (Error error)
      | Some _, Some (Error error) -> resolve (Error error)
      | _ -> ()
    in
    Temporal_future_kernel.observe left (fun result ->
        left_result := Some result;
        finish ());
    Temporal_future_kernel.observe right (fun result ->
        right_result := Some result;
        finish ());
    combined

(** Waits for every input, retaining successful input order and selecting the
    first error in input order only after all inputs settle. *)
let all futures =
  match futures with
  | [] -> (
      match Temporal_runtime.Workflow_context_store.current () with
      | None ->
          make_repr
            ~await:(fun () -> Ok [])
            ~await_gate:(fun register -> register (fun () -> ()))
            ~observe:(fun observer -> observer (Ok []))
            ~is_ready:(fun () -> true) ~peek:(fun () -> Some (Ok []))
            ~owner_id:(-1) ~outside_error:ownership_error
            ~callbacks_live:(fun () -> true)
            ~enqueue:(fun thunk -> thunk ())
      | Some context ->
          let future =
            Temporal_runtime.Workflow_context_store.resolved context (Ok [])
          in
          of_internal
            ~outside_error:(fun () ->
              Error.defect
                ~message:
                  "Temporal future awaited outside its workflow scheduler")
            future)
  | first :: _ when not (same_owner first futures) ->
      ready_like first (Error (ownership_error ()))
  | first :: _ ->
      let combined, resolve =
        make_derived ~parent:first
          ~outside_error:(Temporal_future_kernel.outside_error first)
      in
      let remaining = ref (List.length futures) in
      let results = Array.make !remaining None in
      let finish_if_complete () =
        if !remaining = 0 then
          let ordered = Array.to_list results in
          match
            List.find_map
              (function Some (Error error) -> Some error | _ -> None)
              ordered
          with
          | Some error -> resolve (Error error)
          | None ->
              resolve
                (Ok
                   (List.map
                      (function
                        | Some (Ok value) -> value
                        | Some (Error _) | None ->
                            failwith
                              "Temporal.Future.all result invariant violated")
                      ordered))
      in
      List.iteri
        (fun index future ->
          Temporal_future_kernel.observe future (fun result ->
              results.(index) <- Some result;
              remaining := !remaining - 1;
              finish_if_complete ()))
        futures;
      combined

(** Settles with the first completion of two differently typed inputs. *)
let race left right =
  if Temporal_future_kernel.owner_id left
     <> Temporal_future_kernel.owner_id right then
    ready_like left (Error (ownership_error ()))
  else
    let combined, resolve =
      make_derived ~parent:left
        ~outside_error:(Temporal_future_kernel.outside_error left)
    in
    let settled = ref false in
    let finish wrap result =
      if not !settled then (
        settled := true;
        resolve (Result.map wrap result))
    in
    Temporal_future_kernel.observe left (finish (fun value -> Left value));
    Temporal_future_kernel.observe right (finish (fun value -> Right value));
    combined

(** Settles with the first completion of a non-empty homogeneous collection. *)
let first leading rest =
  if not (same_owner leading rest) then
    ready_like leading (Error (ownership_error ()))
  else
    let combined, resolve =
      make_derived ~parent:leading
        ~outside_error:(Temporal_future_kernel.outside_error leading)
    in
    let settled = ref false in
    let finish result =
      if not !settled then (
        settled := true;
        resolve result)
    in
    List.iter
      (fun future -> Temporal_future_kernel.observe future finish)
      (leading :: rest);
    combined
