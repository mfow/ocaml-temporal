#!/bin/sh
set -eu

# Temporal's SQL tool reads the password from SQL_PASSWORD. Keeping schema
# ownership in the official admin-tools image avoids adding database tooling to
# either the host or the future OCaml worker image.
: "${POSTGRES_SEEDS:?POSTGRES_SEEDS is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${SQL_PASSWORD:?SQL_PASSWORD is required}"

port=${DB_PORT:-5432}

# Creates the database when absent, establishes Temporal's version table, and
# applies every migration newer than the stored version. All three operations
# are deliberately safe to repeat when a retained Compose volume is restarted.
setup_database() {
  database=$1
  schema_directory=$2

  temporal-sql-tool \
    --plugin postgres12 \
    --ep "$POSTGRES_SEEDS" \
    -u "$POSTGRES_USER" \
    -p "$port" \
    --db "$database" \
    create
  temporal-sql-tool \
    --plugin postgres12 \
    --ep "$POSTGRES_SEEDS" \
    -u "$POSTGRES_USER" \
    -p "$port" \
    --db "$database" \
    setup-schema -v 0.0
  temporal-sql-tool \
    --plugin postgres12 \
    --ep "$POSTGRES_SEEDS" \
    -u "$POSTGRES_USER" \
    -p "$port" \
    --db "$database" \
    update-schema -d "$schema_directory"
}

setup_database temporal /etc/temporal/schema/postgresql/v12/temporal/versioned
setup_database temporal_visibility /etc/temporal/schema/postgresql/v12/visibility/versioned
