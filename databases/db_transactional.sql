CREATE DATABASE db_transactional
    WITH
    OWNER = root
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

\c db_transactional

CREATE EXTENSION postgis;


-- Crear data pais
CREATE TABLE IF NOT EXISTS Pais (
    pais_id serial PRIMARY KEY,
    nombre VARCHAR(255),
    geometria GEOMETRY(MultiPolygon, 4326)
);


-- Crear data region
CREATE TABLE IF NOT EXISTS Region (
    region_id serial PRIMARY KEY,
    nombre VARCHAR(255),
    geometria GEOMETRY(MultiPolygon, 4326)

);

CREATE TABLE tempRegion (data jsonb);
COPY tempRegion (data) FROM '/docker-entrypoint-initdb.d/jsonFiles/regionesDB.json';

INSERT INTO Region (region_id, nombre, geometria)
SELECT DISTINCT (data->>'CUT')::INT,
    (data->>'nombre')::VARCHAR(255) as nombre,
    ST_SetSRID(ST_Multi(ST_GeomFromGeoJSON(data->>'geometria')), 4326) as geometria
FROM tempRegion
WHERE NOT EXISTS (
    SELECT 1
    FROM Region r
    WHERE r.region_id = (data->>'region_id')::INT
);
DROP TABLE IF EXISTS tempRegion;

--Crear data comuna
CREATE TABLE IF NOT EXISTS Comuna (
    comuna_id serial PRIMARY KEY,
    nombre VARCHAR(255),
    poblacion INT,
    area FLOAT,
    geometria GEOMETRY(MultiPolygon, 4326),
    region_id INT,
    FOREIGN KEY (region_id) REFERENCES Region(region_id) ON DELETE CASCADE
);


CREATE TABLE tempComuna (data jsonb);
COPY tempComuna (data) FROM '/docker-entrypoint-initdb.d/jsonFiles/comunasDB.json';

INSERT INTO Comuna (comuna_id, nombre, poblacion, area, geometria, region_id)
SELECT DISTINCT (data->>'CUT')::INT,
    (data->>'nombre')::VARCHAR(255) as nombre,
    (data->>'poblacion')::INT as poblacion,
    (data->>'area')::FLOAT as area,
    ST_SetSRID(ST_Multi(ST_GeomFromGeoJSON(data->>'geometria')), 4326)::GEOMETRY as geometria,
    (data->>'region_id')::INT as region_id
FROM tempComuna
WHERE NOT EXISTS (
    SELECT 1
    FROM Comuna c
    WHERE c.comuna_id = (data->>'CUT')::INT
);

DROP TABLE IF EXISTS tempComuna;


--Crear dimensiones
CREATE TABLE IF NOT EXISTS Dimension (
    ID serial PRIMARY KEY,
    nombre VARCHAR(255),
    comuna_id INT NOT NULL,
    FOREIGN KEY (comuna_id) REFERENCES Comuna(comuna_id) ON DELETE CASCADE
);

DO $$

    DECLARE comuna_cursor CURSOR FOR SELECT comuna_id FROM Comuna;
    DECLARE comuna_id INT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM Dimension LIMIT 1) THEN
        OPEN comuna_cursor;
        LOOP
            FETCH comuna_cursor INTO comuna_id;
            IF comuna_id IS NULL THEN
                EXIT;
            END IF;

            INSERT INTO Dimension (nombre, comuna_id)
            VALUES ('Educacional', comuna_id);

            INSERT INTO Dimension (nombre, comuna_id)
            VALUES ('Salud', comuna_id);

            INSERT INTO Dimension (nombre, comuna_id)
            VALUES ('Seguridad', comuna_id);

            INSERT INTO Dimension (nombre, comuna_id)
            VALUES ('Tecnologia', comuna_id);

            INSERT INTO Dimension (nombre, comuna_id)
            VALUES ('Economico', comuna_id);

            INSERT INTO Dimension (nombre, comuna_id)
            VALUES ('Ecologico', comuna_id);

            INSERT INTO Dimension (nombre, comuna_id)
            VALUES ('Movilidad', comuna_id);

            INSERT INTO Dimension (nombre, comuna_id)
            VALUES ('Diversion', comuna_id);

        END LOOP;
        CLOSE comuna_cursor;
    END IF;

END $$;