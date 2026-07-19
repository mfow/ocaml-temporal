#!/bin/sh
set -eu

# Temporal's SQL tool reads the password from SQL_PASSWORD. Keeping schema
# ownership in the official admin-tools image avoids adding database tooling to
# either the host or the future OCaml worker image.
: "${POSTGRES_SEEDS:?POSTGRES_SEEDS is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${SQL_PASSWORD:?SQL_PASSWORD is required}"

port=${DB_PORT:-5432}

# Temporal's SQL tool uses different wording for an already-created database
# and an already-initialized schema across image releases. Normalize the
# diagnostic before matching it so idempotent startup does not depend on
# capitalization or one exact PostgreSQL prefix.
existing_state() {
  printf '%s\n' "$1" \
    | tr '[:upper:]' '[:lower:]' \
    | grep -Eq 'already[[:space:]-]+(exists|initialized|created)|database.*exists|schema.*exists'
}

# SQL-tool startup can briefly race PostgreSQL's transition from accepting
# connections to accepting schema DDL. Keep retries here, at the database
# boundary, so the Temporal service never observes a half-initialized schema.
# A bounded retry also preserves a useful failure when the image, credentials,
# or migration directory is genuinely invalid.
run_sql_tool() {
  allow_existing=$1
  database=$2
  shift 2
  attempts=1
  while :; do
    if output=$(temporal-sql-tool \
      --plugin postgres12 \
      --ep "$POSTGRES_SEEDS" \
      -u "$POSTGRES_USER" \
      -p "$port" \
      --db "$database" "$@" 2>&1); then
      printf '%s\n' "$output"
      return 0
    fi

    if [ "$allow_existing" = yes ]; then
      if existing_state "$output"; then
        printf '%s\n' "$output" >&2
        return 0
      fi
    fi

    if [ "$attempts" -ge 30 ]; then
      printf '%s\n' "$output" >&2
      printf 'temporal-sql-tool %s failed after %s attempts\n' \
        "$*" "$attempts" >&2
      return 1
    fi

    printf 'temporal-sql-tool %s attempt %s failed; retrying\n' \
      "$*" "$attempts" >&2
    attempts=$((attempts + 1))
    sleep 2
  done
}

# Creates the database when absent. PostgreSQL images commonly create the
# user's database during first boot, so an explicit create can legitimately
# report that it already exists; that state is success, not a failed setup.
create_database() {
  database=$1
  attempts=1
  while :; do
    if output=$(temporal-sql-tool \
      --plugin postgres12 \
      --ep "$POSTGRES_SEEDS" \
      -u "$POSTGRES_USER" \
      -p "$port" \
      --db "$database" \
      create 2>&1); then
      printf '%s\n' "$output"
      return 0
    fi
    if existing_state "$output"; then
      printf '%s\n' "$output" >&2
      return 0
    fi
    if [ "$attempts" -ge 30 ]; then
      printf '%s\n' "$output" >&2
      printf 'temporal-sql-tool create failed after %s attempts\n' \
        "$attempts" >&2
      return 1
    fi
    printf 'temporal-sql-tool create attempt %s failed; retrying\n' \
      "$attempts" >&2
    attempts=$((attempts + 1))
    sleep 2
  done
}

# Establishes Temporal's version table and applies every migration newer than
# the stored version. These operations are safe to repeat after a retained
# Compose volume is restarted.
setup_database() {
  database=$1
  schema_directory=$2

  create_database "$database"
  run_sql_tool yes "$database" setup-schema -v 0.0
  run_sql_tool no "$database" update-schema -d "$schema_directory"
}

setup_database temporal /etc/temporal/schema/postgresql/v12/temporal/versioned
setup_database temporal_visibility /etc/temporal/schema/postgresql/v12/visibility/versioned
