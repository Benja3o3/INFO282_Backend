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
    CUT INT PRIMARY KEY,
    nombre VARCHAR(255),
    geometria GEOMETRY(Point, 4326)
);

CREATE TABLE IF NOT EXISTS Comuna (
    CUT INT PRIMARY KEY,
    nombre VARCHAR(255),
    poblacion INT,
    geometria GEOMETRY(Point, 4326),
    region_id INT NOT NULL,
    FOREIGN KEY (region_id) REFERENCES Region(CUT) ON DELETE CASCADE

);


CREATE TABLE IF NOT EXISTS DataEnBruto (
    ID INT PRIMARY KEY,
    valor INT,
    nombre VARCHAR(255),
    fuente VARCHAR(255),
    dimension VARCHAR(255),
    comuna_id INT NOT NULL,
    FOREIGN KEY (comuna_id) REFERENCES Comuna(CUT) ON DELETE CASCADE

);