#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONNECTORS_DIR="$SCRIPT_DIR/connectors"

usage() {
    cat <<USAGE
Usage: $(basename "$0") <command> [connector]

  list                      connectors with connector and task state
  apply [connector...]      create or update from connectors/<name>.json (all if none given)
  validate <connector>      validate a config file without deploying it
  status <connector>        connector and task status
  config <connector>        deployed configuration
  offsets <connector>       committed source offsets
  pause|resume|stop <connector>
  restart <connector>       restart the connector and all its tasks
  delete <connector>        remove the connector (its replication slot is NOT dropped)
  plugins                   installed connector plugins
  snapshot <table>          incremental snapshot of public.<table> via the signal table (re-emits every row)

CONNECT_URL defaults to http://localhost:8083 (use http://localhost:8084 for the second worker).
USAGE
}

api() {
    local method="$1" path="$2"
    shift 2
    curl -sS --fail-with-body -X "$method" -H 'Content-Type: application/json' "$@" "$CONNECT_URL$path"
}

config_file() {
    local file="$CONNECTORS_DIR/$1.json"
    [[ -f "$file" ]] || { echo "No config file: $file" >&2; exit 1; }
    echo "$file"
}

require_name() {
    [[ -n "${1:-}" ]] || { usage >&2; exit 1; }
}

command="${1:-}"
name="${2:-}"

case "$command" in
    list)
        api GET '/connectors?expand=status' \
            | jq -r 'to_entries[] | "\(.key)\t\(.value.status.connector.state)\t\(.value.status.tasks | map("\(.id):\(.state)@\(.worker_id)") | join(" "))"' \
            | column -t -s $'\t'
        ;;
    apply)
        shift
        names=("$@")
        if [[ ${#names[@]} -eq 0 ]]; then
            for file in "$CONNECTORS_DIR"/*.json; do names+=("$(basename "$file" .json)"); done
        fi
        for connector in "${names[@]}"; do
            echo "Applying $connector"
            api PUT "/connectors/$connector/config" --data "@$(config_file "$connector")" | jq -c '{name, type, tasks}'
        done
        ;;
    validate)
        require_name "$name"
        file="$(config_file "$name")"
        class="$(jq -r '."connector.class" | split(".") | last' "$file")"
        jq --arg name "$name" '. + {name: $name}' "$file" \
            | api PUT "/connector-plugins/$class/config/validate" --data @- \
            | jq '{error_count, errors: [.configs[] | select(.value.errors | length > 0) | {(.value.name): .value.errors}]}'
        ;;
    status) require_name "$name"; api GET "/connectors/$name/status" | jq ;;
    config) require_name "$name"; api GET "/connectors/$name/config" | jq ;;
    offsets) require_name "$name"; api GET "/connectors/$name/offsets" | jq ;;
    pause) require_name "$name"; api PUT "/connectors/$name/pause" && echo "Paused $name" ;;
    resume) require_name "$name"; api PUT "/connectors/$name/resume" && echo "Resumed $name" ;;
    stop) require_name "$name"; api PUT "/connectors/$name/stop" && echo "Stopped $name" ;;
    restart) require_name "$name"; api POST "/connectors/$name/restart?includeTasks=true" | jq ;;
    delete) require_name "$name"; api DELETE "/connectors/$name" && echo "Deleted $name" ;;
    snapshot)
        require_name "$name"
        signal="{\"data-collections\": [\"public.$name\"], \"type\": \"incremental\"}"
        docker compose -f "$SCRIPT_DIR/../docker-compose.yml" exec -T postgres \
            psql -U pesaflow -d pesaflow -v ON_ERROR_STOP=1 -c \
            "insert into debezium_signals (id, type, data) values ('snapshot-$name-' || now()::text, 'execute-snapshot', '$signal')"
        ;;
    plugins) api GET '/connector-plugins' | jq -r '.[] | "\(.type)\t\(.class)\t\(.version)"' | column -t -s $'\t' ;;
    *) usage; [[ -z "$command" || "$command" == "help" ]] || exit 1 ;;
esac
