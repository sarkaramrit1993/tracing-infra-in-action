#!/usr/bin/env bash
# Counts the objects behind the cold partition twice: once as ClickHouse
# records them, once as SeaweedFS stores them.
#
# Usage: ./scripts/count-cold-objects.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

PART=$(tiering_partition)
[ "$(partition_disk "$PART")" = s3_cold ] \
  || die "partition $PART is not on the cold volume: run ./scripts/move-partition-to-cold.sh first"

echo "ClickHouse:"
ch_table "
SELECT count() AS s3_objects, formatReadableSize(sum(size)) AS bytes
FROM system.remote_data_paths
WHERE disk_name = 's3_cold'
  AND splitByChar('/', local_path)[3] = (
        SELECT toString(uuid) FROM system.tables
        WHERE database = 'tracing' AND name = 'tiering_demo')
  AND splitByChar('/', local_path)[-2] IN (
        SELECT name FROM system.parts
        WHERE database = 'tracing' AND table = 'tiering_demo' AND active
          AND partition = '$PART' AND disk_name = 's3_cold')"
echo
echo "SeaweedFS:"
echo "fs.du /buckets/traces-cold" | docker compose exec -T seaweedfs weed shell 2> /dev/null | grep '^block:'
echo "fs.tree /buckets/traces-cold" | docker compose exec -T seaweedfs weed shell 2> /dev/null | grep 'files$'
