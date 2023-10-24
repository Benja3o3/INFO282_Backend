#!/bin/sh

# Iniciar el servicio vsftpd en segundo plano
/sbin/tini -s -- /bin/start_vsftpd.sh &

# Iniciar el proxy de proceso pidproxy en segundo plano
pidproxy &

# Iniciar el servicio cron
crond -f