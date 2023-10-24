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


CREATE TABLE IF NOT EXISTS Pais (
    pais_id INT PRIMARY KEY
);

INSERT INTO Pais (pais_id)
VALUES (1);

CREATE TABLE IF NOT EXISTS Region (
    region_id INT PRIMARY KEY,
    pais_id INT,
    FOREIGN KEY (pais_id) REFERENCES Pais(pais_id) ON DELETE CASCADE
);

CREATE TABLE tempRegion (data jsonb);
COPY tempRegion (data) FROM '/docker-entrypoint-initdb.d/jsonFiles/regionesDB.json';

INSERT INTO Region (region_id, pais_id)
SELECT DISTINCT (data->>'CUT')::INT, 1 AS pais_id
FROM tempRegion
WHERE NOT EXISTS (
    SELECT 1
    FROM Region r
    WHERE r.region_id = (data->>'region_id')::INT
);
DROP TABLE IF EXISTS tempRegion;



CREATE TABLE IF NOT EXISTS Comuna (
    comuna_id INT PRIMARY KEY,
    region_id INT,
    FOREIGN KEY (region_id) REFERENCES region(region_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Dimension (
    ID serial PRIMARY KEY,
    nombre VARCHAR(255),
    dimension_id INT,
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

INSERT INTO Comuna (comuna_id, region_id)
SELECT DISTINCT (data->>'CUT')::INT,
    (data->>'region_id')::INT as region_id
FROM tempComuna
WHERE NOT EXISTS (
    SELECT 1
    FROM Comuna c
    WHERE c.comuna_id = (data->>'CUT')::INT
);
DROP TABLE IF EXISTS tempComuna;

