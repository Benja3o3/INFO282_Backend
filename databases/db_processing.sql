CREATE DATABASE db_processing
    WITH
    OWNER = root
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

\c db_processing 

CREATE TABLE IF NOT EXISTS Comuna (
    CUT INT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS Dimension (
    ID serial PRIMARY KEY,
    nombre VARCHAR(255),
    valor FLOAT,
    comuna_id int NOT NULL,
    FOREIGN KEY (comuna_id) REFERENCES Comuna(CUT) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Indicador (
    ID serial PRIMARY KEY,
    nombre VARCHAR(255),
    prioridad VARCHAR(255),
    fuente VARCHAR(255),
    valor FLOAT,
    fecha DATE,
    dimension_id INT NOT NULL,
    FOREIGN KEY (dimension_id) REFERENCES Dimension(ID) ON DELETE CASCADE
);




CREATE TABLE tempComuna (data jsonb);
COPY tempComuna (data) FROM '/docker-entrypoint-initdb.d/jsonFiles/comunasDB.json';

INSERT INTO Comuna
SELECT (data->>'CUT')::INT
FROM tempComuna;

INSERT INTO Comuna (CUT)
SELECT DISTINCT (data->>'CUT')::INT
FROM tempComuna
WHERE NOT EXISTS (
    SELECT 1
    FROM Comuna c
    WHERE c.CUT = (data->>'CUT')::INT
);

DROP TABLE IF EXISTS tempComuna;



DO $$

    DECLARE comuna_cursor CURSOR FOR SELECT CUT FROM Comuna;
    DECLARE comuna_id INT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM Dimension LIMIT 1) THEN
        OPEN comuna_cursor;
        LOOP
            FETCH comuna_cursor INTO comuna_id;
            IF comuna_id IS NULL THEN
                EXIT;
            END IF;

            INSERT INTO Dimension (nombre, valor, comuna_id)
            VALUES ('Educacional', 0, comuna_id);

            INSERT INTO Dimension (nombre, valor, comuna_id)
            VALUES ('Salud', 0, comuna_id);

            INSERT INTO Dimension (nombre, valor, comuna_id)
            VALUES ('Seguridad', 0, comuna_id);

            INSERT INTO Dimension (nombre, valor, comuna_id)
            VALUES ('Tecnologia', 0, comuna_id);

            INSERT INTO Dimension (nombre, valor, comuna_id)
            VALUES ('Economico', 0, comuna_id);

            INSERT INTO Dimension (nombre, valor, comuna_id)
            VALUES ('Ecologico', 0, comuna_id);

            INSERT INTO Dimension (nombre, valor, comuna_id)
            VALUES ('Movilidad', 0, comuna_id);

            INSERT INTO Dimension (nombre, valor, comuna_id)
            VALUES ('Diversion', 0, comuna_id);

        END LOOP;
        CLOSE comuna_cursor;
    END IF;

END $$;