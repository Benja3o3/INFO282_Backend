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


CREATE TABLE IF NOT EXISTS Indicador (
    ID serial PRIMARY KEY,
    nombre VARCHAR(255),
    prioridad VARCHAR(255),
    fuente VARCHAR(255),
    valor FLOAT,
    fecha DATE,
    comuna_id INT NOT NULL,
    FOREIGN KEY (comuna_id) REFERENCES Comuna(CUT) ON DELETE CASCADE

);


CREATE TABLE IF NOT EXISTS Dimension (
    ID serial PRIMARY KEY,
    nombre VARCHAR(255),
    valor FLOAT,
    indicador_id int NOT NULL,
    FOREIGN KEY (indicador_id) REFERENCES Indicador(ID) ON DELETE CASCADE
);


CREATE TABLE tempComuna (data jsonb);
COPY tempComuna (data) FROM '/docker-entrypoint-initdb.d/jsonFiles/comunasDB.json';


INSERT INTO Comuna (CUT)
SELECT DISTINCT (data->>'CUT')::INT
FROM tempComuna
WHERE NOT EXISTS (
    SELECT 1
    FROM Comuna c
    WHERE c.CUT = (data->>'CUT')::INT
);

DROP TABLE IF EXISTS tempComuna;
