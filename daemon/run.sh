#!/bin/sh
/sbin/tini -s -- ./vsftpd.sh &
pidproxy &
crond -f