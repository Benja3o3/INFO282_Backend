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

CREATE TABLE IF NOT EXISTS Regiones (
    region_id INT PRIMARY KEY,
    pais_id INT,
    FOREIGN KEY (pais_id) REFERENCES Pais(pais_id) ON DELETE CASCADE
);

CREATE TABLE tempRegion (data jsonb);
COPY tempRegion (data) FROM '/docker-entrypoint-initdb.d/jsonFiles/regionesDB.json';

INSERT INTO Regiones (region_id, pais_id)
SELECT DISTINCT (data->>'CUT')::INT, 1 AS pais_id
FROM tempRegion
WHERE NOT EXISTS (
    SELECT 1
    FROM Regiones r
    WHERE r.region_id = (data->>'region_id')::INT
);
DROP TABLE IF EXISTS tempRegion;



CREATE TABLE IF NOT EXISTS Comunas (
    comuna_id INT PRIMARY KEY,
    region_id INT,
    FOREIGN KEY (region_id) REFERENCES Regiones(region_id) ON DELETE CASCADE
);

CREATE TABLE tempComuna (data jsonb);
COPY tempComuna (data) FROM '/docker-entrypoint-initdb.d/jsonFiles/comunasDB.json';

INSERT INTO Comunas (comuna_id, region_id)
SELECT DISTINCT (data->>'CUT')::INT,
    (data->>'region_id')::INT as region_id
FROM tempComuna
WHERE NOT EXISTS (
    SELECT 1
    FROM Comunas c
    WHERE c.comuna_id = (data->>'CUT')::INT
);
DROP TABLE IF EXISTS tempComuna;


CREATE TABLE IF NOT EXISTS Dimensiones (
    dimension_id serial PRIMARY KEY,
    nombre VARCHAR(255),
    descripcion VARCHAR(255),
    peso FLOAT
);

INSERT INTO Dimensiones (nombre, descripcion, peso)
VALUES ('Educacional', 'Destinada a la educacion poblacion', 1),
        ('Salud', 'Destinada a la salud de la poblacion', 1),
        ('Seguridad', 'Centrada en que tan segura se siente la población', 1),
        ('Tecnologia', 'Destinada en la infraestructura tecnologica de la población', 1),
        ('Economico', 'Destinada al nivel socioeconomico de la población', 1),
        ('Ecologico', 'Destinada al area ambiental de la población', 1),
        ('Movilidad', 'Destinada a la capacidad de movilidad de la población', 1),
        ('Diversion', 'Destinada al apartado de diversion de la población', 1);


CREATE TABLE IF NOT EXISTS dimensionescomunas (
    PRIMARY KEY (comuna_id, dimension_id),
    comuna_id INT NOT NULL,
    dimension_id INT NOT NULL,
    FOREIGN KEY (comuna_id) REFERENCES Comunas(comuna_id) ON DELETE CASCADE,
    FOREIGN KEY (dimension_id) REFERENCES Dimensiones(dimension_id) ON DELETE CASCADE
);

INSERT INTO dimensionescomunas (comuna_id, dimension_id)
SELECT c.comuna_id, d.dimension_id
FROM Comunas c
CROSS JOIN Dimensiones d;



CREATE TABLE IF NOT EXISTS calculoDimensiones (
    calculoDimension_id SERIAL PRIMARY KEY,
    valor FLOAT,
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY (comuna_id, dimension_id) REFERENCES dimensionescomunas(comuna_id, dimension_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Indicadoresinfo (
    indicadoresinfo_id INT NOT NULL,
    nombre VARCHAR(255),
    prioridad FLOAT,
    descripcion VARCHAR(255),
    fuente VARCHAR(255), 
    dimension INT,
    PRIMARY KEY (indicadoresinfo_id)
);

CREATE TABLE IF NOT EXISTS indicadoresCalculo (
    calculoindicador_id SERIAL PRIMARY KEY,
    valor FLOAT,
    fecha DATE,
    flag BOOLEAN,
    dimension_id INT NOT NULL,
    comuna_id INT NOT NULL,
    indicador_id INT NOT NULL,
    FOREIGN KEY (comuna_id, dimension_id) REFERENCES dimensionescomunas(comuna_id, dimension_id) ON DELETE CASCADE,
    FOREIGN KEY (indicador_id) REFERENCES Indicadoresinfo(indicadoresinfo_id) ON DELETE CASCADE
);

CREATE INDEX idx_join ON indicadoresCalculo (comuna_id, dimension_id);
