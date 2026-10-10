#!/usr/bin/env bash
# Smoke-test the test Kafka from docker/compose.test.yml: from a separate container on
# the kafka4php-test network, create a topic, produce 3 records and consume them back.
# Exits non-zero on failure. Requires `docker compose -f docker/compose.test.yml up -d --wait`.
set -euo pipefail
export MSYS_NO_PATHCONV=1

image="$(docker compose -f "$(dirname "${BASH_SOURCE[0]}")/../docker/compose.test.yml" config --images | head -n1)"
topic="k4p-check-$(date +%s)-$$"

docker run --rm -i --network kafka4php-test -e TOPIC="$topic" "$image" bash -euo pipefail -c '
    bin=/opt/kafka/bin
    bs=kafka:9092
    $bin/kafka-topics.sh --bootstrap-server $bs --create --topic "$TOPIC" --partitions 3 --replication-factor 1
    printf "one\ntwo\nthree\n" | $bin/kafka-console-producer.sh --bootstrap-server $bs --topic "$TOPIC" >/dev/null
    got="$($bin/kafka-console-consumer.sh --bootstrap-server $bs --topic "$TOPIC" \
        --from-beginning --max-messages 3 --timeout-ms 30000 2>/dev/null | sort | tr "\n" " ")"
    $bin/kafka-topics.sh --bootstrap-server $bs --delete --topic "$TOPIC"
    if [ "$got" = "one three two " ]; then
        echo "kafka-check: OK (created $TOPIC, produced and consumed 3 records)"
    else
        echo "kafka-check: FAILED, consumed: [$got]" >&2
        exit 1
    fi
'
