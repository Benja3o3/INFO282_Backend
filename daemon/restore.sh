#!/bin/ash

DATABASE_HOST=databases
POSTGRES_USER=uach
POSTGRES_PASSWORD=uachbienestar2023
BACKUP_DIR=/daemon/backup
mkdir -p $BACKUP_DIR

export PGPASSWORD=$POSTGRES_PASSWORD

BACKUP_FILE_TRANSACTIONAL=$(ls -t $BACKUP_DIR/backup_db_transactional_*.sql 2>/dev/null | head -n1)
BACKUP_FILE_PROCESSING=$(ls -t $BACKUP_DIR/backup_db_processing_*.sql 2>/dev/null | head -n1)

# Verificar si existen los archivos de respaldo
if [ -n "$BACKUP_FILE_TRANSACTIONAL" ] && [ -n "$BACKUP_FILE_PROCESSING" ]; then
    # Número máximo de intentos
    MAX_ATTEMPTS=5
    # Intervalo entre intentos en segundos
    SLEEP_INTERVAL=5

    attempt=1
    while [ $attempt -le $MAX_ATTEMPTS ]; do
        echo "Intento $attempt de restauración..."
        # Eliminar restricciones existentes en db_transactional
        psql -h $DATABASE_HOST -U $POSTGRES_USER -d db_transactional -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"

        # Restaurar base de datos db_transactional
        pg_restore -h $DATABASE_HOST -U $POSTGRES_USER -d db_transactional -v "$BACKUP_FILE_TRANSACTIONAL" && break

        echo "Error en el intento $attempt. Esperando $SLEEP_INTERVAL segundos antes de volver a intentar..."
        sleep $SLEEP_INTERVAL

        attempt=$((attempt + 1))
    done

    # Si el bucle se ejecuta el número máximo de veces sin éxito
    if [ $attempt -gt $MAX_ATTEMPTS ]; then
        echo "Error: No se pudo restaurar la base de datos db_transactional después de $MAX_ATTEMPTS intentos."
    else
        echo "Restauración de db_transactional completa."
    fi

    attempt=1
    while [ $attempt -le $MAX_ATTEMPTS ]; do
        echo "Intento $attempt de restauración..."
        # Eliminar restricciones existentes en db_processing
        psql -h $DATABASE_HOST -U $POSTGRES_USER -d db_processing -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
        # Restaurar base de datos db_processing
        pg_restore -h $DATABASE_HOST -U $POSTGRES_USER -d db_processing -v "$BACKUP_FILE_PROCESSING" && break

        echo "Error en el intento $attempt. Esperando $SLEEP_INTERVAL segundos antes de volver a intentar..."
        sleep $SLEEP_INTERVAL

        attempt=$((attempt + 1))
    done

    # Si el bucle se ejecuta el número máximo de veces sin éxito
    if [ $attempt -gt $MAX_ATTEMPTS ]; then
        echo "Error: No se pudo restaurar la base de datos db_processing después de $MAX_ATTEMPTS intentos."
    else
        echo "Restauración de db_processing completa."
    fi
else
    echo "No se encontraron archivos de respaldo. No se realizará ninguna restauración."
fi



unset PGPASSWORD
