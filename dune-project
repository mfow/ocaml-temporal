(lang dune 3.17)
(name temporal)
(generate_opam_files false)
(source (github ocaml-temporal/ocaml-temporal))
(license Apache-2.0)
(authors "OCaml Temporal contributors")
(maintainers "OCaml Temporal contributors")

(package
 (name temporal)
 (allow_empty)
 (synopsis "Temporal workflows in modern OCaml")
 (description "A typed OCaml 5 workflow SDK backed by Temporal Core")
 (depends
  (ocaml (>= 5.2))
  (dune (>= 3.17))))
