#!/bin/sh
# Wraps the image's entrypoint to grant the configured user access to named
# collections. The user is defined in XML by the image (from CLICKHOUSE_USER),
# XML users cannot receive SQL GRANTs, and XML element names cannot be read
# from the environment, so the override file is written here at start-up.
set -eu

cat > /etc/clickhouse-server/users.d/named_collections.xml <<EOF
<clickhouse>
    <users>
        <${CLICKHOUSE_USER}>
            <named_collection_control>1</named_collection_control>
        </${CLICKHOUSE_USER}>
    </users>
</clickhouse>
EOF

exec /entrypoint.sh "$@"
