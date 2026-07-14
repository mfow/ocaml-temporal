(** Negative installed-package fixture: a consumer must not be able to import
    the scheduler-owned Future kernel directly. The compile-contract harness
    expects this module alias to fail when the consumer links only
    [temporal-sdk], proving that the internal type-identity exception remains
    hidden behind the public [Temporal.Future] facade. *)
module Private = Temporal_future_kernel
