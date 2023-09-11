CREATE EXTENSION postgis;

CREATE TABLE IF NOT EXISTS Region (
    CUT int PRIMARY KEY,
    Nombre VARCHAR(255),
    Geometria GEOMETRY(Point, 4326)
);

CREATE TABLE IF NOT EXISTS Comuna (
    CUT int PRIMARY KEY,
    Nombre VARCHAR(255),
    Poblacion int,
    Geometria GEOMETRY(Point, 4326)
);

-- Insertar datos de prueba en la tabla Region
INSERT INTO Region (CUT, Nombre, Geometria)
VALUES
    (1, 'Región Metropolitana', ST_SetSRID(ST_MakePoint(-70.6483, -33.4372), 4326)),
    (2, 'Región de Valparaíso', ST_SetSRID(ST_MakePoint(-71.6483, -32.9372), 4326)),
    (3, 'Región del Biobío', ST_SetSRID(ST_MakePoint(-73.0483, -36.8372), 4326));

-- Insertar datos de prueba en la tabla Comuna
INSERT INTO Comuna (CUT, Nombre, Poblacion, Geometria)
VALUES
    (13101, 'Santiago', 503147, ST_SetSRID(ST_MakePoint(-70.6483, -33.4372), 4326)),
    (5101, 'Valparaíso', 315732, ST_SetSRID(ST_MakePoint(-71.6483, -32.9372), 4326)),
    (8101, 'Concepción', 215413, ST_SetSRID(ST_MakePoint(-73.0483, -36.8372), 4326));
