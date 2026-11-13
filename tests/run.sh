#!/bin/bash
# End-to-end test: builds the image and runs the real bridge in it against fake WhatsApp and
# Sprites APIs, stand-in Claudes, and a real Postgres. About ten minutes. Needs Docker.
#   tests/run.sh
set -u
cd "$(dirname "$0")/.."
IMAGE=agent-e2e:test
docker build --platform linux/amd64 -q -t "$IMAGE" . > /dev/null || exit 1
docker rm -f agent-e2e agent-pg-test > /dev/null 2>&1
docker run -d --name agent-pg-test -e POSTGRES_PASSWORD=pw -p 55432:5432 postgres:17-alpine > /dev/null
for _ in $(seq 1 30); do  # the image restarts Postgres once after initialising, so retry until the schema applies
  sleep 2
  sed "s/REPLACE-WITH-A-LONG-RANDOM-PASSWORD/test-pw/" supabase/schema.sql \
    | docker exec -i agent-pg-test psql -U postgres -v ON_ERROR_STOP=1 -q > /dev/null 2>&1 && break
done
docker run -d --name agent-e2e --platform linux/amd64 -p 18999:18999 -v "$PWD/tests":/tests:ro "$IMAGE" bash /tests/container.sh > /dev/null
sleep 10
python3 tests/scenarios.py
rc=$?
[ $rc -eq 0 ] || docker logs agent-e2e 2>&1 | tail -40
docker rm -f agent-e2e agent-pg-test > /dev/null 2>&1
exit $rc
