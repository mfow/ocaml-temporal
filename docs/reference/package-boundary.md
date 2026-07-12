# Installed package boundary

`temporal-sdk` has one supported OCaml API: the wrapped `Temporal` library.
The worker, runtime, JSON protocol, mailbox, supervisor, and C/Rust bridge are
implementation details of that library. An application links `temporal-sdk`
and writes ordinary `Temporal.Client`, `Temporal.Worker`, workflow, activity,
and codec code; it does not depend on the implementation libraries directly.

## Dune invariant

Every implementation library linked by `lib/public/dune` is declared with:

```lisp
(package temporal-sdk)
```

and has no `public_name`. This applies to:

- `temporal_base`;
- `temporal_protocol`;
- `temporal_core_bridge`;
- `temporal_runtime`;
- `temporal_mailbox_processor`; and
- `temporal_sdk_supervisor`.

Dune therefore installs their archives and interfaces below the package's
reserved `__private__/` directory. They are package-private dependencies of
`temporal-sdk`, not separately installable findlib libraries. The public
library remains the only top-level package name an application should request.
The private modules may still be present in an installed artifact because the
public archive needs them at link time; their location and package metadata
prevent normal consumers from importing them by module or library name.

`private_modules` in `lib/public/dune` is complementary: it keeps selected
modules out of the generated `Temporal` signature. It is not a substitute for
the package-private dependency declaration because it cannot hide a separate
library's archive or interface.

The native bridge follows the same rule. The installed artifact contains the
static archive needed to link an OCaml-owned executable, but does not install
the C header or Rust source. No public type exposes a Rust handle, protobuf,
JSON bridge record, mailbox, or supervisor state.

## Regression evidence

The installed-package smoke test is deliberately run against a fresh consumer
directory rather than against the repository source tree:

```sh
make test-install
```

`test/bridge/test_install.sh` checks both sides of the boundary:

1. it verifies that every implementation archive is below
   `temporal-sdk/__private__/` and that no matching top-level artifact exists;
2. it builds and runs a small consumer using `Temporal.Runtime_info`, proving
   the public package and linked static bridge remain usable; and
3. it separately compiles fixtures that try to name `Mailbox_processor`,
   `Sdk_supervisor`, and `Temporal_core_bridge` through only
   `(libraries temporal-sdk)`. Each compilation must fail because those module
   paths are not part of the consumer's normal include path.

The repository smoke test also checks every internal Dune stanza for the
`(package temporal-sdk)` declaration and rejects a future `public_name`.
Changes that intentionally publish an implementation component must update
this document, the public API review, and the install regression rather than
silently widening the package surface.
