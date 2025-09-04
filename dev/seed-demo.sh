#!/bin/bash
set -e
ROOT=${ADLS_FS:-./data}
runId=$(uuidgen)
run=$ROOT/runs/$runId
now=$(date -u +%Y/%m/%d/%H/%M)/0
for step in a b c; do
  dir=$run/ready/$now
  mkdir -p "$dir"
  cat > "$dir/step-$step.json" <<JSON
{"runId":"$runId","stepId":"$step","taskType":"sleep.randomFail","dependsOn":[],"remainingDeps":0,"dueAt":"$(date -u +%FT%TZ)","attempts":0,"timeoutMs":1000,"maxAttempts":3,"inputRef":null,"outputRef":null,"idempotencyKey":null}
JSON
  mkdir -p "$run/io/$step"
  echo '{}' > "$run/io/$step/in.json"
done
mkdir -p "$run/deps"
echo 2 > "$run/deps/child.cnt"
cat > "$run/ready/$now/step-child.json" <<JSON
{"runId":"$runId","stepId":"child","taskType":"sleep.randomFail","dependsOn":["a","b"],"remainingDeps":2,"dueAt":"$(date -u +%FT%TZ)","attempts":0,"timeoutMs":1000,"maxAttempts":3,"inputRef":null,"outputRef":null,"idempotencyKey":null}
JSON
echo "runId=$runId"
