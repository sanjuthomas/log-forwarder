#!/bin/sh
set -e

# Mounted volumes (named or bind) are often root-owned; ensure the forwarder user can write.
for dir in /state /output /dlq; do
	if [ -d "$dir" ]; then
		chown -R forwarder:forwarder "$dir"
	fi
done

exec su-exec forwarder /usr/local/bin/log-forwarder "$@"
