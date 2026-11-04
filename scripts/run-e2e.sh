#!/bin/sh
set -eu

project_root=$(CDPATH= cd "$(dirname "$0")/.." && pwd)
cd "$project_root"
artifacts=${E2E_ARTIFACTS_DIR:-"$project_root/artifacts/e2e"}
mkdir -p "$artifacts"
run_state=$(mktemp "${TMPDIR:-/tmp}/flight-booking-e2e.XXXXXX")
run_token=$(basename "$run_state")
container="${COMPOSE_PROJECT_NAME:-soa-flight-booking}-$run_token"
label=io.soa-flight-booking.e2e-run

collect_and_remove() {
    test_status=$?
    trap - EXIT
    copy_status=0
    remove_status=0
    owner=$(docker inspect --format '{{ index .Config.Labels "io.soa-flight-booking.e2e-run" }}' "$container" 2>/dev/null) || owner=
    if [ "$owner" = "$run_token" ]; then
        docker cp "$container:/artifacts/." "$artifacts/" || copy_status=$?
        docker rm -f "$container" >/dev/null || remove_status=$?
    else
        echo 'E2E container was not created; no artifacts to collect.' >&2
        copy_status=1
    fi
    rm -f "$run_state"
    if [ "$test_status" -eq 0 ]; then
        if [ "$copy_status" -ne 0 ]; then test_status=$copy_status; else test_status=$remove_status; fi
    fi
    exit "$test_status"
}
trap collect_and_remove EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Copy through Docker so artifacts also reach the host with a remote daemon.
docker compose run --build --name "$container" --label "$label=$run_token" e2e "$@"
