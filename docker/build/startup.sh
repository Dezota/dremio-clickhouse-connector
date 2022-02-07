#!/bin/bash
chown -R dremio:dremio /var/lib/dremio
touch /var/log/dremio/server.log
/opt/dremio/bin/dremio start
tail -f /var/log/dremio/server.log
