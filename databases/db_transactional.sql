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

CREATE TABLE IF NOT EXISTS Region (
    CUT serial PRIMARY KEY,
    nombre VARCHAR(255),
    geometria GEOMETRY(MultiPolygon, 4326)
);

CREATE TABLE IF NOT EXISTS Comuna (
    CUT serial PRIMARY KEY,
    nombre VARCHAR(255),
    poblacion INT,
    geometria GEOMETRY(MultiPolygon, 4326),
    region_id INT,
    FOREIGN KEY (region_id) REFERENCES Region(CUT) ON DELETE CASCADE

);


CREATE TABLE IF NOT EXISTS DataEnBruto (
    ID serial PRIMARY KEY,
    valor INT,
    nombre VARCHAR(255),
    fuente VARCHAR(255),
    comuna_id INT NOT NULL,
    FOREIGN KEY (comuna_id) REFERENCES Comuna(CUT) ON DELETE CASCADE

);

CREATE TABLE tempRegion (data jsonb);
COPY tempRegion (data) FROM '/docker-entrypoint-initdb.d/jsonFiles/regionesDB.json';

INSERT INTO Region
SELECT (data->>'CUT')::INT, 
(data ->> 'nombre')::VARCHAR(255) as nombre, 
ST_SetSRID(ST_Multi(ST_GeomFromGeoJSON(data->>'geometria')), 4326)
FROM tempRegion;




CREATE TABLE tempComuna (data jsonb);
COPY tempComuna (data) FROM '/docker-entrypoint-initdb.d/jsonFiles/comunasDB.json';

INSERT INTO Comuna
SELECT (data->>'CUT')::INT, 
        (data ->> 'nombre')::VARCHAR(255) as nombre, 
        (data->> 'poblacion')::INT, 
        ST_SetSRID(ST_Multi(ST_GeomFromGeoJSON(data->>'geometria')), 4326)::GEOMETRY,
        (data ->> 'region_id')::INT
FROM tempComuna;
