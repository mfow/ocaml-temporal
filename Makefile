TEMPORAL_FIXTURE_DIR := $(CURDIR)/test/integration/temporal
TEMPORAL_COMPOSE_FILE := $(TEMPORAL_FIXTURE_DIR)/compose.yaml
TEMPORAL_COMPOSE_PROJECT ?= ocaml-temporal-integration
COMPOSE := docker compose --project-directory "$(TEMPORAL_FIXTURE_DIR)" --file "$(TEMPORAL_COMPOSE_FILE)" --project-name "$(TEMPORAL_COMPOSE_PROJECT)"
# Compose services bind-mount the repository. Propagate the invoking user's
# numeric identity, selected OCaml image, and bounded driver timeout so every
# service shares Dune's lock/build ownership and overrides behave predictably.
TEMPORAL_COMPOSE = OCAML_IMAGE=$(OCAML_IMAGE) HOST_UID=$(HOST_UID) HOST_GID=$(HOST_GID) SMOKE_DRIVER_TIMEOUT_SECONDS=$(TEMPORAL_DRIVER_TIMEOUT_SECONDS) $(COMPOSE) --profile temporal
TEMPORAL_DRIVER_TIMEOUT_SECONDS ?= 120
SMOKE_DRIVER_LOG_FILE := $(TEMPORAL_FIXTURE_DIR)/.smoke-driver.log
SERVICE ?= dev
OCAML_VERSION ?= 5.2
OCAML_IMAGE ?= ocaml/opam:debian-12-ocaml-$(OCAML_VERSION)
HOST_UID ?= $(shell id -u)
HOST_GID ?= $(shell id -g)
# Leave Dune's worker count unchanged by default. A constrained Docker VM can
# set `DUNE_JOBS=1` (or another small value) to avoid concurrent native linkers
# exhausting its memory without changing the normal CI/default behavior.
DUNE_JOBS ?=
DUNE_BUILD_ARGS := $(if $(strip $(DUNE_JOBS)),-j $(DUNE_JOBS),)
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
QUALITY_CARGO_DENY_VERSION ?= 0.20.2
QUALITY_CARGO_MACHETE_VERSION ?= 0.9.2
QUALITY_TYPOS_VERSION ?= 1.48.0

.PHONY: version-check build cargo-metadata test test-unit test-runtime test-rust test-bridge test-install test-quality-contract test-temporal-config test-core-lifecycle-integration temporal-start temporal-start-worker temporal-run-driver temporal-inspect-smoke test-temporal-two-binary test-temporal-integration temporal-health temporal-status temporal-logs temporal-stop temporal-clean lint lint-rust fmt quality quality-tool-version-check quality-rust quality-spelling license-check audit clean verify check native-version-check native-build native-test native-test-rust native-test-install native-lint native-lint-rust native-verify
version-check:
	@actual="$$( $(RUN) ocamlc -version | tail -n 1 )"; \
	case "$$actual" in \
		$(OCAML_VERSION).*) ;; \
		*) echo "expected OCaml $(OCAML_VERSION).x, got $$actual" >&2; exit 1 ;; \
	esac

build:
	$(RUN) dune build $(DUNE_BUILD_ARGS)
	$(CARGO) build --manifest-path $(CARGO_MANIFEST) --locked

# Emits only the locked Cargo metadata document on stdout, allowing CI to pipe
# it into the isolated license scanner without knowing the Compose fixture path.
cargo-metadata:
	@$(CARGO) metadata --manifest-path $(CARGO_MANIFEST) --locked --format-version 1

test:
	$(MAKE) test-temporal-config
	$(RUN) dune runtest
	$(MAKE) test-rust
	$(MAKE) test-bridge
	$(MAKE) test-install
	$(MAKE) test-quality-contract

test-rust:
	$(COMPOSE_RUN) sh test/smoke/test_rust_toolchain.sh
	$(CARGO) test --manifest-path $(CARGO_MANIFEST) --locked

test-bridge:
	$(COMPOSE_RUN) sh test/bridge/test_abi.sh

test-install:
	$(COMPOSE_RUN) sh test/bridge/test_install.sh

test-quality-contract:
	sh test/smoke/test_quality_contract.sh .

test-temporal-config:
	sh test/smoke/test_temporal_compose_config.sh

# This command runs inside the OCaml development container but connects to the
# real Temporal service on the shared Compose network.
test-core-lifecycle-integration:
	$(COMPOSE_RUN) env TEMPORAL_ADDRESS=http://temporal:7233 TEMPORAL_NAMESPACE=temporal-sdk-test opam exec -- dune exec test/integration/test_core_lifecycle.exe

# The live stack is intentionally separate from unit verification. Native
# Windows and macOS jobs build the SDK directly and never start Linux services.
temporal-start:
	$(TEMPORAL_COMPOSE) up --detach --wait postgresql temporal
	$(MAKE) temporal-health

# Starts the long-lived OCaml worker only after the database and Temporal
# frontend are healthy. The worker health check is backed by an atomic marker
# published after public Worker.create succeeds, not merely by process liveness.
temporal-start-worker:
	$(TEMPORAL_COMPOSE) up --detach --build --wait smoke-worker

# Runs the independent OCaml driver against the already-running worker. Using
# `run --no-deps` avoids accidentally creating a second worker process and makes
# the driver's exit status the acceptance test's authoritative result. The
# bounded timeout turns a lost native request into a normal nonzero child exit
# instead of allowing CI to wait for the job's global timeout. Capturing the
# one-off container output in a host file lets the failure trap print driver
# phases even though `--rm` removes the container before Compose logs can see it.
temporal-run-driver:
	@set -eu; \
	rm -f "$(SMOKE_DRIVER_LOG_FILE)"; \
	status=0; \
	$(TEMPORAL_COMPOSE) run --rm --no-deps smoke-driver >"$(SMOKE_DRIVER_LOG_FILE)" 2>&1 || status=$$?; \
	cat "$(SMOKE_DRIVER_LOG_FILE)"; \
	if [ "$$status" -ne 0 ]; then $(MAKE) temporal-inspect-smoke || true; $(MAKE) temporal-logs || true; fi; \
	exit "$$status"

# Performs a failure-only metadata check for the four known workflow IDs: the
# driver's three top-level executions and its deterministic child. The
# admin-tools output contains execution status/run identity but no history or
# payloads, which distinguishes a start, worker-dispatch, and terminal-wait
# failure without expanding the acceptance test's privacy surface. A missing
# workflow is expected when the driver stalled before its first start, so every
# query is best effort and cannot mask the original exit status.
temporal-inspect-smoke:
	@for workflow_id in two-binary-fan-out two-binary-timer-then-activity two-binary-parent-awaits-child two-binary-parent-child-smoke; do \
		echo "--- Temporal metadata for $$workflow_id ---"; \
		$(TEMPORAL_COMPOSE) run --rm --no-deps temporal-admin-tools \
			temporal workflow describe --workflow-id "$$workflow_id" \
			--namespace temporal-sdk-test || true; \
	done

temporal-health:
	$(TEMPORAL_COMPOSE) exec -T postgresql pg_isready -U temporal -d postgres
	$(TEMPORAL_COMPOSE) exec -T postgresql psql -v ON_ERROR_STOP=1 -U temporal -d temporal -tAc 'SELECT 1 FROM schema_version LIMIT 1'
	$(TEMPORAL_COMPOSE) exec -T postgresql psql -v ON_ERROR_STOP=1 -U temporal -d temporal_visibility -tAc 'SELECT 1 FROM schema_version LIMIT 1'
	$(TEMPORAL_COMPOSE) run --rm --no-deps --entrypoint /bin/sh temporal-admin-tools /scripts/check-temporal-stack.sh

temporal-status:
	$(TEMPORAL_COMPOSE) ps

temporal-logs:
	$(TEMPORAL_COMPOSE) logs --no-color --tail 200 postgresql temporal-schema temporal smoke-worker
	@if [ -f "$(SMOKE_DRIVER_LOG_FILE)" ]; then \
		echo '--- smoke-driver one-off output ---'; \
		tail -n 200 "$(SMOKE_DRIVER_LOG_FILE)"; \
	fi

temporal-stop:
	$(TEMPORAL_COMPOSE) down --remove-orphans

temporal-clean:
	$(TEMPORAL_COMPOSE) down --volumes --remove-orphans
	@rm -f "$(SMOKE_DRIVER_LOG_FILE)"

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
	$(MAKE) temporal-health; \
	$(MAKE) test-core-lifecycle-integration; \
	$(MAKE) temporal-start-worker; \
	$(MAKE) temporal-run-driver

# Explicit name for callers that want to discover the two-process acceptance
# without reading the broader integration target. It shares the same isolated
# lifecycle and therefore cannot leave a second Temporal stack behind.
test-temporal-two-binary: test-temporal-integration

test-unit:
	$(RUN) dune runtest test/unit test/smoke

test-runtime:
	$(RUN) dune runtest test/runtime

lint:
	$(RUN) dune build $(DUNE_BUILD_ARGS)
	$(COMPOSE_RUN) sh scripts/check-format.sh
	$(MAKE) lint-rust

lint-rust:
	$(CARGO) fmt --manifest-path $(CARGO_MANIFEST) --all -- --check
	$(CARGO) clippy --manifest-path $(CARGO_MANIFEST) --locked --all-targets -- -D warnings

fmt:
	$(COMPOSE_RUN) sh scripts/check-format.sh

# These release binaries are intentionally separate from the development
# image. CI installs their pinned, checksum-verified artifacts once, while a
# contributor can install the same versions and invoke the identical Make gate.
quality: quality-rust quality-spelling

quality-tool-version-check:
	@actual="$$(cargo deny --version | awk '{ print $$2 }')"; \
	if [ "$$actual" != "$(QUALITY_CARGO_DENY_VERSION)" ]; then \
		echo "expected cargo-deny $(QUALITY_CARGO_DENY_VERSION), got $$actual" >&2; exit 1; \
	fi
	@actual="$$(cargo machete --version | awk '{ print $$1 }')"; \
	if [ "$$actual" != "$(QUALITY_CARGO_MACHETE_VERSION)" ]; then \
		echo "expected cargo-machete $(QUALITY_CARGO_MACHETE_VERSION), got $$actual" >&2; exit 1; \
	fi
	@actual="$$(typos --version | awk '{ print $$2 }')"; \
	if [ "$$actual" != "$(QUALITY_TYPOS_VERSION)" ]; then \
		echo "expected typos $(QUALITY_TYPOS_VERSION), got $$actual" >&2; exit 1; \
	fi

quality-rust: quality-tool-version-check
	cargo deny --manifest-path $(CARGO_MANIFEST) --locked --all-features check advisories sources
	cargo machete --with-metadata rust

quality-spelling: quality-tool-version-check
	typos

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
	$(NATIVE_ENV) $(NATIVE_RUN) dune build @install $(DUNE_BUILD_ARGS)
	$(NATIVE_ENV) cargo build --manifest-path $(CARGO_MANIFEST) --locked

native-test: native-test-rust native-test-install test-quality-contract
	$(NATIVE_ENV) $(NATIVE_RUN) dune runtest

native-test-rust:
	$(NATIVE_ENV) cargo test --manifest-path $(CARGO_MANIFEST) --locked

native-test-install:
	$(NATIVE_ENV) sh test/bridge/test_install.sh

native-lint: native-lint-rust
	$(NATIVE_ENV) $(NATIVE_RUN) dune build $(DUNE_BUILD_ARGS)
	sh scripts/check-format.sh

native-lint-rust:
	$(NATIVE_ENV) cargo fmt --manifest-path $(CARGO_MANIFEST) --all -- --check
	$(NATIVE_ENV) cargo clippy --manifest-path $(CARGO_MANIFEST) --locked --all-targets -- -D warnings

native-verify: native-version-check native-build native-lint native-test
