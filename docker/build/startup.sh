#!/bin/bash
/opt/dremio/bin/dremio start
tail -f /var/log/dremio/server.out
