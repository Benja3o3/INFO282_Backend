CREATE DATABASE db_transactional
    WITH
    OWNER = uach
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
    pais_id INT PRIMARY KEY,
    nombre VARCHAR(255),
    geometria GEOMETRY(MultiPolygon, 4326)
);
INSERT INTO Pais (pais_id)
VALUES (1);

-- Crear data region
CREATE TABLE IF NOT EXISTS Region (
    region_id serial PRIMARY KEY,
    nombre VARCHAR(255),
    geometria GEOMETRY(MultiPolygon, 4326),
    pais_id INT,
    FOREIGN KEY (pais_id) REFERENCES Pais(pais_id) ON DELETE CASCADE

);

CREATE TABLE tempRegion (data jsonb);
COPY tempRegion (data) FROM '/docker-entrypoint-initdb.d/jsonFiles/regionesDB.json';

INSERT INTO Region (region_id, nombre, geometria, pais_id)
SELECT DISTINCT (data->>'CUT')::INT,
    (data->>'nombre')::VARCHAR(255) as nombre,
    ST_SetSRID(ST_Multi(ST_GeomFromGeoJSON(data->>'geometria')), 4326) as geometria,
    1 as pais_id
FROM tempRegion
WHERE NOT EXISTS (
    SELECT 1
    FROM Region r
    WHERE r.region_id = (data->>'region_id')::INT
);
DROP TABLE IF EXISTS tempRegion;

--Crear data comuna
CREATE TABLE IF NOT EXISTS Comunas (
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

INSERT INTO Comunas (comuna_id, nombre, poblacion, area, geometria, region_id)
SELECT DISTINCT (data->>'CUT')::INT,
    (data->>'nombre')::VARCHAR(255) as nombre,
    (data->>'poblacion')::INT as poblacion,
    (data->>'area')::FLOAT as area,
    ST_SetSRID(ST_Multi(ST_GeomFromGeoJSON(data->>'geometria')), 4326)::GEOMETRY as geometria,
    (data->>'region_id')::INT as region_id
FROM tempComuna
WHERE NOT EXISTS (
    SELECT 1
    FROM Comunas c
    WHERE c.comuna_id = (data->>'CUT')::INT
);

DROP TABLE IF EXISTS tempComuna;


--Crear dimensiones
CREATE TABLE IF NOT EXISTS Dimensiones (
    dimension_id serial PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL
);

INSERT INTO Dimensiones (nombre)
VALUES ('Educacional'),
        ('Salud'),
        ('Seguridad'),
        ('Tecnologia'),
        ('Economico'),
        ('Ecologico'),
        ('Movilidad'),
        ('Diversion');

CREATE TABLE IF NOT EXISTS comunasdimensiones (
    comuna_id INT,
    dimension_id INT,
    PRIMARY KEY (comuna_id, dimension_id),
    FOREIGN KEY (comuna_id) REFERENCES Comunas (comuna_id) ON DELETE CASCADE,
    FOREIGN KEY (dimension_id) REFERENCES dimensiones (dimension_id) ON DELETE CASCADE
);

INSERT INTO comunasdimensiones (comuna_id, dimension_id)
SELECT c.comuna_id, d.dimension_id
FROM Comunas c
CROSS JOIN Dimensiones d;


