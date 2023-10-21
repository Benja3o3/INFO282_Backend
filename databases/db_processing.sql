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

CREATE TABLE IF NOT EXISTS Region (
    region_id INT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS Comuna (
    comuna_id INT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS Dimension (
    ID serial PRIMARY KEY,
    nombre VARCHAR(255),
    valor FLOAT,
    fecha DATE,
    flag BOOLEAN, 
    comuna_id INT NOT NULL,
    FOREIGN KEY (comuna_id) REFERENCES Comuna(comuna_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Indicadores (
    ID VARCHAR(255) PRIMARY KEY,
    nombre VARCHAR(255),
    prioridad INT,
    dimension VARCHAR(255),
    fuente VARCHAR(255)
);


CREATE TABLE tempComuna (data jsonb);
COPY tempComuna (data) FROM '/docker-entrypoint-initdb.d/jsonFiles/comunasDB.json';

INSERT INTO Comuna
SELECT (data->>'CUT')::INT
FROM tempComuna;

INSERT INTO Comuna (comuna_id)
SELECT DISTINCT (data->>'CUT')::INT
FROM tempComuna
WHERE NOT EXISTS (
    SELECT 1
    FROM Comuna c
    WHERE c.comuna_id = (data->>'CUT')::INT
);

DROP TABLE IF EXISTS tempComuna;

