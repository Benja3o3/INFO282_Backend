#!/bin/ash
DATABASE_HOST=databases
POSTGRES_USER=uach
POSTGRES_PASSWORD=uachbienestar2023
BACKUP_DIR=/daemon/backup
mkdir -p $BACKUP_DIR

BACKUP_FILE="backup_$(date +\%Y\%m\%d).sql"
export PGPASSWORD=$POSTGRES_PASSWORD
pg_dump -h $DATABASE_HOST -U $POSTGRES_USER -F c -b -v -f $BACKUP_DIR/$BACKUP_FILE
unset PGPASSWORD
