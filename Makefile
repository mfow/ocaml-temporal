COMPOSE := docker compose
TEMPORAL_COMPOSE := $(COMPOSE) --profile temporal
SERVICE ?= dev
OCAML_VERSION ?= 5.2
OCAML_IMAGE ?= ocaml/opam:debian-12-ocaml-$(OCAML_VERSION)
HOST_UID ?= $(shell id -u)
HOST_GID ?= $(shell id -g)
COMPOSE_RUN := OCAML_IMAGE=$(OCAML_IMAGE) $(COMPOSE) --progress quiet run --rm --build --user $(HOST_UID):$(HOST_GID) $(SERVICE)
RUN := $(COMPOSE_RUN) opam exec --
CARGO := $(COMPOSE_RUN) cargo
CARGO_MANIFEST := rust/Cargo.toml
NATIVE_RUN := opam exec --
NATIVE_CARGO_TARGET_DIR ?= $(CARGO_TARGET_DIR)
ifeq ($(strip $(NATIVE_CARGO_TARGET_DIR)),)
NATIVE_CARGO_TARGET_DIR := $(CURDIR)/_build/rust
endif
NATIVE_OCAML_VERSION ?= 5.5
NATIVE_RUST_VERSION ?= 1.94.1
NATIVE_ARCH ?=
NATIVE_RUST_HOST ?=
NATIVE_ENV := CARGO_TARGET_DIR="$(NATIVE_CARGO_TARGET_DIR)"

.PHONY: version-check build test test-unit test-runtime test-rust test-bridge test-install test-temporal-config test-temporal-integration temporal-start temporal-health temporal-status temporal-logs temporal-stop temporal-clean lint lint-rust fmt license-check audit clean verify check native-version-check native-build native-test native-test-rust native-test-install native-lint native-lint-rust native-verify
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
	$(MAKE) test-temporal-config
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

test-temporal-config:
	sh test/smoke/test_temporal_compose_config.sh

# The live stack is intentionally separate from unit verification. Native
# Windows and macOS jobs build the SDK directly and never start Linux services.
temporal-start:
	$(TEMPORAL_COMPOSE) up --detach --wait postgresql temporal
	$(MAKE) temporal-health

temporal-health:
	$(TEMPORAL_COMPOSE) exec -T postgresql pg_isready -U temporal -d postgres
	$(TEMPORAL_COMPOSE) exec -T postgresql psql -v ON_ERROR_STOP=1 -U temporal -d temporal -tAc 'SELECT 1 FROM schema_version LIMIT 1'
	$(TEMPORAL_COMPOSE) exec -T postgresql psql -v ON_ERROR_STOP=1 -U temporal -d temporal_visibility -tAc 'SELECT 1 FROM schema_version LIMIT 1'
	$(TEMPORAL_COMPOSE) run --rm --no-deps --entrypoint /bin/sh temporal-admin-tools /scripts/check-temporal-stack.sh

temporal-status:
	$(TEMPORAL_COMPOSE) ps

temporal-logs:
	$(TEMPORAL_COMPOSE) logs --no-color --tail 200 postgresql temporal-schema temporal

temporal-stop:
	$(TEMPORAL_COMPOSE) down --remove-orphans

temporal-clean:
	$(TEMPORAL_COMPOSE) down --volumes --remove-orphans

test-temporal-integration: test-temporal-config
	@set -eu; \
	cleanup() { \
		status=$$?; \
		trap - EXIT HUP INT TERM; \
		if [ "$$status" -ne 0 ]; then \
			$(MAKE) temporal-logs || true; \
		fi; \
		$(MAKE) temporal-clean || true; \
		exit "$$status"; \
	}; \
	$(MAKE) temporal-clean; \
	trap cleanup EXIT; \
	trap 'exit 129' HUP; \
	trap 'exit 130' INT; \
	trap 'exit 143' TERM; \
	$(MAKE) temporal-start; \
	$(MAKE) temporal-health

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
	$(TEMPORAL_COMPOSE) down --remove-orphans
	rm -rf _build

verify: version-check lint test

check: verify license-check

# Native targets are used by the non-Linux compatibility jobs. The default
# developer path remains Docker Compose so no host OCaml toolchain is required.
native-version-check:
	@actual_ocaml="$$( $(NATIVE_RUN) ocamlc -version )"; \
	case "$$actual_ocaml" in \
		$(NATIVE_OCAML_VERSION).*) ;; \
		*) echo "expected OCaml $(NATIVE_OCAML_VERSION).x, got $$actual_ocaml" >&2; exit 1 ;; \
	esac
	@actual_rust="$$(rustc --version | awk '{ print $$2 }')"; \
	if [ "$$actual_rust" != "$(NATIVE_RUST_VERSION)" ]; then \
		echo "expected rustc $(NATIVE_RUST_VERSION), got $$actual_rust" >&2; exit 1; \
	fi
	@if [ -n "$(NATIVE_ARCH)" ]; then \
		actual_arch="$$( $(NATIVE_RUN) ocamlc -config-var architecture )"; \
		if [ "$$actual_arch" != "$(NATIVE_ARCH)" ]; then \
			echo "expected OCaml architecture $(NATIVE_ARCH), got $$actual_arch" >&2; exit 1; \
		fi; \
	fi
	@if [ -n "$(NATIVE_RUST_HOST)" ]; then \
		actual_host="$$(rustc -vV | sed -n 's/^host: //p')"; \
		if [ "$$actual_host" != "$(NATIVE_RUST_HOST)" ]; then \
			echo "expected Rust host $(NATIVE_RUST_HOST), got $$actual_host" >&2; exit 1; \
		fi; \
	fi

native-build:
	$(NATIVE_ENV) $(NATIVE_RUN) dune build @install
	$(NATIVE_ENV) cargo build --manifest-path $(CARGO_MANIFEST) --locked

native-test: native-test-rust native-test-install
	$(NATIVE_ENV) $(NATIVE_RUN) dune runtest

native-test-rust:
	$(NATIVE_ENV) cargo test --manifest-path $(CARGO_MANIFEST) --locked

native-test-install:
	$(NATIVE_ENV) sh test/bridge/test_install.sh

native-lint: native-lint-rust
	$(NATIVE_ENV) $(NATIVE_RUN) dune build
	sh scripts/check-format.sh

native-lint-rust:
	$(NATIVE_ENV) cargo fmt --manifest-path $(CARGO_MANIFEST) --all -- --check
	$(NATIVE_ENV) cargo clippy --manifest-path $(CARGO_MANIFEST) --locked --all-targets -- -D warnings

native-verify: native-version-check native-build native-lint native-test
