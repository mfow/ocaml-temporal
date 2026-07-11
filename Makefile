COMPOSE := docker compose
RUN := $(COMPOSE) run --rm dev opam exec --

.PHONY: build test test-unit test-runtime lint fmt clean verify
build:
	$(RUN) dune build

test:
	$(RUN) dune runtest

test-unit:
	$(RUN) dune runtest test/unit test/smoke

test-runtime:
	$(RUN) dune runtest test/runtime

lint:
	$(RUN) dune build @fmt

fmt:
	$(RUN) dune fmt

clean:
	$(COMPOSE) down --remove-orphans
	rm -rf _build

verify: lint test
