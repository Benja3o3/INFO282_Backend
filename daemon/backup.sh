#!/bin/ash
DATABASE_HOST=databases
POSTGRES_USER=uach
POSTGRES_PASSWORD=uachbienestar2023
BACKUP_DIR=/daemon/backup
mkdir -p $BACKUP_DIR


CURRENT_DATE=$(date +\%Y\%m\%d)
export PGPASSWORD=$POSTGRES_PASSWORD

BACKUP_FILE_TRANSACTIONAL="backup_db_transactional_${CURRENT_DATE}.sql"
pg_dump -h $DATABASE_HOST -U $POSTGRES_USER -d db_transactional -F c -b -v -f $BACKUP_DIR/$BACKUP_FILE_TRANSACTIONAL


BACKUP_FILE_PROCESSING="backup_db_processing_${CURRENT_DATE}.sql"
pg_dump -h $DATABASE_HOST -U $POSTGRES_USER -d db_processing -F c -b -v -f $BACKUP_DIR/$BACKUP_FILE_PROCESSING

unset PGPASSWORD



