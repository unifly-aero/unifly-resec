#!/usr/bin/env bash
# Simulates the real layout: resec and a Consul client agent run on each client
# machine and stay up. The admin machines (Consul servers) fail in different ways.
# The observer (http://localhost:8090) is never touched and runs for the full test.
#
#   demo/scenario.sh            run all scenarios
#   demo/scenario.sh quorum     run one scenario (quorum | gone | hang | replaced)
set -u
cd "$(dirname "$0")/.."
C="docker compose -f demo/docker-compose.v2.yml"
SERVERS="consul-server-1 consul-server-2 consul-server-3"
AGENT=consul-resec-1
RESULTS=()
CHECKS=()               # "<scenario>|PASS|<text>" for the observer page
CUR=baseline            # current scenario
RUN=()                  # scenarios in this run
declare -A STATE        # scenario -> pending | running | passing | failing
RESULTS_FILE=demo/observer/results.json
OUTAGE=${OUTAGE:-90}     # seconds, longer than lock TTL (15s) and monitor window (30s)
SETTLE=${SETTLE:-120}    # max seconds to wait after the servers are back (session invalidation 30s + lock-delay 15s + election)

log()    { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
marker() { log "=== $*"; $C exec -T $AGENT sh -c "echo '=== $*' >> /proc/1/fd/1" 2>/dev/null || true; }
health() { curl -s -m 10 -o /dev/null -w '%{http_code}' "localhost:$1/health"; }
role()   { $C exec -T "$1" redis-cli role 2>/dev/null | head -1 | tr -d '\r'; }
master_of() { $C exec -T "$1" redis-cli role 2>/dev/null | sed -n 2p | tr -d '\r'; }
masters_in_catalog() { curl -s -m 5 'localhost:8500/v1/health/service/redis-master?passing' | grep -o '"ServiceID":"redis@[^"]*"' | wc -l; }
resec_masters() { for p in 8081 8082; do curl -s -m 5 localhost:$p/state; echo; done | grep -o '"Master":true' | wc -l; }
lock_session() { curl -s -m 5 localhost:8500/v1/kv/resec/.lock | grep -o '"Session":"[^"]*"' | wc -l; }
count_log() { $C logs --since "$1" "$2" 2>/dev/null | grep -c "$3"; }

check() { # check <name> <expected> <actual>
  if [ "$2" = "$3" ]; then RESULTS+=("PASS  $1 = $3"); CHECKS+=("$CUR|PASS|$1 = $3"); log "PASS $1 = $3"
  else RESULTS+=("FAIL  $1 expected $2 got $3"); CHECKS+=("$CUR|FAIL|$1 expected $2 got $3"); log "FAIL $1 expected $2 got $3"; fi
  write_results
}

title() {
  case $1 in
    baseline) echo "Baseline - cluster is up" ;;
    quorum)   echo "Scenario 1 - quorum loss (2 of 3 servers stopped)" ;;
    gone)     echo "Scenario 2 - all servers stopped" ;;
    hang)     echo "Scenario 3 - all servers hang (paused)" ;;
    replaced) echo "Scenario 4 - servers replaced with empty ones" ;;
    *)        echo "$1" ;;
  esac
}

json_str() { local v=${1//\\/\\\\}; v=${v//\"/\\\"}; printf '"%s"' "$v"; }

write_results() { # writes the scenario results for the observer page
  local out sc sep csep c st txt
  out="{\"updated\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"scenarios\":["
  sep=""
  for sc in baseline "${RUN[@]}"; do
    out+="$sep{\"id\":$(json_str "$sc"),\"title\":$(json_str "$(title "$sc")"),\"status\":\"${STATE[$sc]:-pending}\",\"checks\":["
    csep=""
    for c in "${CHECKS[@]}"; do
      [ "${c%%|*}" = "$sc" ] || continue
      st=${c#*|}; txt=${st#*|}; st=${st%%|*}
      out+="$csep{\"ok\":$([ "$st" = PASS ] && echo true || echo false),\"text\":$(json_str "$txt")}"
      csep=","
    done
    out+="]}"; sep=","
  done
  out+="]}"
  printf '%s\n' "$out" > "$RESULTS_FILE.tmp" && mv "$RESULTS_FILE.tmp" "$RESULTS_FILE"
}

begin() { CUR=$1; STATE[$1]=running; write_results; }
finish() { # sets passing or failing for the current scenario
  if printf '%s\n' "${CHECKS[@]}" | grep -q "^$CUR|FAIL|"; then STATE[$CUR]=failing; else STATE[$CUR]=passing; fi
  write_results
}

snapshot() { # prints roles and health
  log "resec-1 /health=$(health 8081)  resec-2 /health=$(health 8082)  redis-1=$(role redis-resec-1)/$(master_of redis-resec-1)  redis-2=$(role redis-resec-2)/$(master_of redis-resec-2)  masters=$(masters_in_catalog) lock=$(lock_session)"
}

expect_steady() { # <prefix>  cluster is healthy and consistent
  check "$1 resec-1 health" 200 "$(health 8081)"
  check "$1 resec-2 health" 200 "$(health 8082)"
  local r1 r2; r1=$(role redis-resec-1); r2=$(role redis-resec-2)
  check "$1 one redis master" 1 "$(printf '%s\n%s\n' "$r1" "$r2" | grep -c '^master$')"
  check "$1 one redis slave" 1 "$(printf '%s\n%s\n' "$r1" "$r2" | grep -c '^slave$')"
  if [ "$r1" = slave ]; then check "$1 slave follows master" redis-resec-2 "$(master_of redis-resec-1)"; fi
  if [ "$r2" = slave ]; then check "$1 slave follows master" redis-resec-1 "$(master_of redis-resec-2)"; fi
  check "$1 masters in catalog" 1 "$(masters_in_catalog)"
  check "$1 lock held" 1 "$(lock_session)"
  check "$1 one resec master" 1 "$(resec_masters)"
}

expect_frozen() { # <prefix> <role1> <role2>  during outage: health fails, topology unchanged
  check "$1 resec-1 health" 503 "$(health 8081)"
  check "$1 resec-2 health" 503 "$(health 8082)"
  check "$1 redis-1 role unchanged" "$2" "$(role redis-resec-1)"
  check "$1 redis-2 role unchanged" "$3" "$(role redis-resec-2)"
}

expect_no_damage() { # <prefix> <since>
  check "$1 no self-enslave resec-1" 0 "$(count_log "$2" resec-1 'slave of redis-resec-1')"
  check "$1 no self-enslave resec-2" 0 "$(count_log "$2" resec-2 'slave of redis-resec-2')"
  check "$1 no healthy-flap during outage" 0 "$FLAPS"
  check "$1 no crash resec-1" 0 "$(count_log "$2" resec-1 'panic\|fatal')"
  check "$1 no crash resec-2" 0 "$(count_log "$2" resec-2 'panic\|fatal')"
}

wait_steady() { # <max-seconds>  waits until the cluster is steady for 3 polls in a row
  # the catalog can show stale state for a few seconds after the servers are
  # back, so the steady state must hold for 3 polls in a row
  local max=$1 ok=0 i r1 r2
  log "waiting up to ${max}s to settle"
  for i in $(seq 1 $((max / 5))); do
    r1=$(role redis-resec-1); r2=$(role redis-resec-2)
    if [ "$(health 8081)" = 200 ] && [ "$(health 8082)" = 200 ] && [ "$(masters_in_catalog)" = 1 ] \
       && [ "$(lock_session)" = 1 ] && [ "$(resec_masters)" = 1 ] \
       && [ "$(printf '%s\n%s\n' "$r1" "$r2" | grep -c '^master$')" = 1 ]; then
      ok=$((ok + 1)); [ $ok -ge 3 ] && break
    else
      ok=0
    fi
    sleep 5
  done
  log "settled after $((i * 5))s"; snapshot
}

run_outage() { # <name> <break-cmd> <restore-cmd>
  local name=$1 brk=$2 restore=$3 start r1 r2
  begin "$name"
  start=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  r1=$(role redis-resec-1); r2=$(role redis-resec-2)
  marker "$name: start (redis-1=$r1 redis-2=$r2)"
  eval "$brk"
  log "waiting ${OUTAGE}s with the outage"
  sleep 20; snapshot
  sleep $((OUTAGE - 20)); snapshot
  expect_frozen "$name/outage" "$r1" "$r2"
  FLAPS=$(( $(count_log "$start" resec-1 'healthy again') + $(count_log "$start" resec-2 'healthy again') ))
  marker "$name: restoring servers"
  eval "$restore"
  wait_steady "$SETTLE"
  expect_steady "$name/after"
  expect_no_damage "$name" "$start"
  finish
}

scenario_quorum() {
  run_outage quorum \
    "$C stop consul-server-2 consul-server-3 >/dev/null 2>&1" \
    "$C start consul-server-2 consul-server-3 >/dev/null 2>&1"
}

scenario_gone() { # all admin machines down, agents keep running
  run_outage gone \
    "$C stop $SERVERS >/dev/null 2>&1" \
    "$C start $SERVERS >/dev/null 2>&1"
}

scenario_hang() { # admin machines hang: TCP accepts, no answer
  run_outage hang \
    "$C pause $SERVERS >/dev/null 2>&1" \
    "$C unpause $SERVERS >/dev/null 2>&1"
}

scenario_replaced() { # admin machines are gone for good, new ones with empty data
  run_outage replaced \
    "$C rm -sf $SERVERS >/dev/null 2>&1" \
    "$C up -d $SERVERS >/dev/null 2>&1; sleep 15; $C restart consul-resec-1 consul-resec-2 >/dev/null 2>&1"
}

main() {
  [ $# -eq 0 ] && set -- quorum gone hang replaced
  RUN=("$@")
  begin baseline
  # a stack left running from the last run keeps a stale lock session and a
  # stale master service, so always start from a clean stack
  log "removing the old stack"
  $C down -v --remove-orphans >/dev/null 2>&1
  log "starting stack (observer: http://localhost:8090)"
  $C up -d --build >/dev/null 2>&1
  wait_steady 120
  expect_steady baseline
  finish
  for s in "$@"; do
    "scenario_$s"
  done
  echo; echo "================ RESULT ================"
  printf '%s\n' "${RESULTS[@]}"
  echo "fails: $(printf '%s\n' "${RESULTS[@]}" | grep -c '^FAIL')"
  log "stack left running for inspection. Stop with: $C down -v"
}
main "$@"
