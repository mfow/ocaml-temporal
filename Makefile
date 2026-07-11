COMPOSE := docker compose
RUN := $(COMPOSE) run --rm dev opam exec --

.PHONY: build test test-unit test-runtime lint fmt license-check clean verify
build:
	$(RUN) dune build

test:
	$(RUN) dune runtest

test-unit:
	$(RUN) dune runtest test/unit test/smoke

test-runtime:
	$(RUN) dune runtest test/runtime

lint:
	$(RUN) dune build
	$(COMPOSE) run --rm dev sh scripts/check-format.sh

fmt:
	$(COMPOSE) run --rm dev sh scripts/check-format.sh

license-check:
	$(COMPOSE) run --rm dev sh scripts/check-licenses.sh

clean:
	$(COMPOSE) down --remove-orphans
	rm -rf _build

verify: lint license-check test
