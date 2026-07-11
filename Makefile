COMPOSE := docker compose
SERVICE ?= dev
OCAML_VERSION ?= 5.2
OCAML_IMAGE ?= ocaml/opam:debian-12-ocaml-$(OCAML_VERSION)
HOST_UID ?= $(shell id -u)
HOST_GID ?= $(shell id -g)
COMPOSE_RUN := OCAML_IMAGE=$(OCAML_IMAGE) $(COMPOSE) --progress quiet run --rm --build --user $(HOST_UID):$(HOST_GID) $(SERVICE)
RUN := $(COMPOSE_RUN) opam exec --
CARGO := $(COMPOSE_RUN) cargo
CARGO_MANIFEST := rust/Cargo.toml

.PHONY: version-check build test test-unit test-runtime test-rust test-bridge test-install lint lint-rust fmt license-check audit clean verify check
version-check:
	@actual="$$( $(RUN) ocamlc -version | tail -n 1 )"; \
	case "$$actual" in \
		$(OCAML_VERSION).*) ;; \
		*) echo "expected OCaml $(OCAML_VERSION).x, got $$actual" >&2; exit 1 ;; \
	esac

build:
	$(RUN) dune build
	$(CARGO) build --manifest-path $(CARGO_MANIFEST) --locked

test:
	$(RUN) dune runtest
	$(MAKE) test-rust
	$(MAKE) test-bridge
	$(MAKE) test-install

test-rust:
	$(COMPOSE_RUN) sh test/smoke/test_rust_toolchain.sh
	$(CARGO) test --manifest-path $(CARGO_MANIFEST) --locked

test-bridge:
	$(COMPOSE_RUN) sh test/bridge/test_abi.sh

test-install:
	$(COMPOSE_RUN) sh test/bridge/test_install.sh

test-unit:
	$(RUN) dune runtest test/unit test/smoke

test-runtime:
	$(RUN) dune runtest test/runtime

lint:
	$(RUN) dune build
	$(COMPOSE_RUN) sh scripts/check-format.sh
	$(MAKE) lint-rust

lint-rust:
	$(CARGO) fmt --manifest-path $(CARGO_MANIFEST) --all -- --check
	$(CARGO) clippy --manifest-path $(CARGO_MANIFEST) --locked --all-targets -- -D warnings

fmt:
	$(COMPOSE_RUN) sh scripts/check-format.sh

license-check:
	$(COMPOSE_RUN) sh scripts/check-licenses.sh

audit: license-check

clean:
	$(COMPOSE) down --remove-orphans
	rm -rf _build

verify: version-check lint test

check: verify license-check
