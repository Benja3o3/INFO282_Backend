CREATE DATABASE db_processing
    WITH
    OWNER = uach
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

\c db_processing 

-- Informacion
CREATE TABLE IF NOT EXISTS DimensionesInfo (
    dimension_id serial PRIMARY KEY,
    nombre VARCHAR(255),
    descripcion VARCHAR(255),
    peso FLOAT
);

INSERT INTO DimensionesInfo (nombre, descripcion, peso)
VALUES ('Educacional', 'Destinada a la educacion poblacion', 1),
        ('Salud', 'Destinada a la salud de la poblacion', 1),
        ('Seguridad', 'Centrada en que tan segura se siente la población', 1),
        ('Tecnologia', 'Destinada en la infraestructura tecnologica de la población', 1),
        ('Economico', 'Destinada al nivel socioeconomico de la población', 1),
        ('Ecologico', 'Destinada al area ambiental de la población', 1),
        ('Social', 'Destinada al apartado de social de la población', 1);


CREATE TABLE IF NOT EXISTS Indicadoresinfo (
    indicadoresinfo_id INT NOT NULL,
    nombre VARCHAR(255),
    prioridad FLOAT,
    descripcion VARCHAR(255),
    fuente VARCHAR(255), 
    dimension INT,
    PRIMARY KEY (indicadoresinfo_id)
);

-- Localidades

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
    nombre VARCHAR(255),
    region_id INT,
    FOREIGN KEY (region_id) REFERENCES Regiones(region_id) ON DELETE CASCADE
);

CREATE TABLE tempComuna (data jsonb);
COPY tempComuna (data) FROM '/docker-entrypoint-initdb.d/jsonFiles/comunasDB.json';

INSERT INTO Comunas (comuna_id, nombre, region_id)
SELECT DISTINCT (data->>'CUT')::INT,
    (data->>'nombre')::VARCHAR(255) as nombre,
    (data->>'region_id')::INT as region_id
FROM tempComuna
WHERE NOT EXISTS (
    SELECT 1
    FROM Comunas c
    WHERE c.comuna_id = (data->>'CUT')::INT
);
DROP TABLE IF EXISTS tempComuna;

-- Calculo comunas
CREATE TABLE IF NOT EXISTS dimensionescomunas (
    PRIMARY KEY (comuna_id, dimension_id),
    comuna_id INT NOT NULL,
    dimension_id INT NOT NULL,
    FOREIGN KEY (comuna_id) REFERENCES Comunas(comuna_id) ON DELETE CASCADE,
    FOREIGN KEY (dimension_id) REFERENCES DimensionesInfo(dimension_id) ON DELETE CASCADE
);

INSERT INTO dimensionescomunas (comuna_id, dimension_id)
SELECT c.comuna_id, d.dimension_id
FROM Comunas c
CROSS JOIN DimensionesInfo d;

CREATE TABLE IF NOT EXISTS calculodimensionescomuna (
    calculoDimension_id SERIAL PRIMARY KEY,
    valor FLOAT,
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY (comuna_id, dimension_id) REFERENCES dimensionescomunas(comuna_id, dimension_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS calculoindicadorescomuna (
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

CREATE INDEX idx_comuna_dimension
ON calculoindicadorescomuna (comuna_id, flag);

CREATE TABLE IF NOT EXISTS calculobienestarComuna (
    bienestar_id SERIAL PRIMARY KEY,
    comuna_id INT NOT NULL,
    valor_bienestar FLOAT,
    flag BOOLEAN,
    fecha DATE, 
    FOREIGN KEY (comuna_id) REFERENCES Comunas(comuna_id) ON DELETE CASCADE
);


-- Calculo Region
CREATE TABLE IF NOT EXISTS dimensionesregiones (
    PRIMARY KEY (dimension_id, region_id),
    region_id INT NOT NULL,
    dimension_id INT NOT NULL,
    FOREIGN KEY (region_id) REFERENCES Regiones(region_id) ON DELETE CASCADE,
    FOREIGN KEY (dimension_id) REFERENCES DimensionesInfo(dimension_id) ON DELETE CASCADE
);

INSERT INTO dimensionesregiones (region_id, dimension_id)
SELECT r.region_id, d.dimension_id
FROM Regiones r
CROSS JOIN DimensionesInfo d;

CREATE TABLE IF NOT EXISTS calculodimensionesregion (
    calculoDimension_id SERIAL PRIMARY KEY,
    valor FLOAT,
    fecha DATE,
    flag BOOLEAN,
    region_id INT,
    dimension_id INT,
    FOREIGN KEY (region_id, dimension_id) REFERENCES dimensionesregiones(region_id, dimension_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS calculoindicadoresRegion (
    calculoindicador_id SERIAL PRIMARY KEY,
    valor FLOAT,
    fecha DATE,
    flag BOOLEAN,
    dimension_id INT NOT NULL,
    region_id INT NOT NULL,
    indicador_id INT NOT NULL,
    FOREIGN KEY (region_id, dimension_id) REFERENCES dimensionesregiones(region_id, dimension_id) ON DELETE CASCADE,
    FOREIGN KEY (indicador_id) REFERENCES Indicadoresinfo(indicadoresinfo_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS calculobienestarregion (
    bienestar_id SERIAL PRIMARY KEY,
    region_id INT NOT NULL,
    valor_bienestar FLOAT,
    flag BOOLEAN,
    fecha DATE, 
    FOREIGN KEY (region_id) REFERENCES Regiones(region_id) ON DELETE CASCADE
);

-- Calculo pais

CREATE TABLE IF NOT EXISTS dimensionesPais (
    PRIMARY KEY (dimension_id, pais_id),
    pais_id INT NOT NULL,
    dimension_id INT NOT NULL,
    FOREIGN KEY (pais_id) REFERENCES Pais(pais_id) ON DELETE CASCADE,
    FOREIGN KEY (dimension_id) REFERENCES DimensionesInfo(dimension_id) ON DELETE CASCADE
);

INSERT INTO dimensionespais (pais_id, dimension_id)
SELECT p.pais_id, d.dimension_id
FROM Pais p
CROSS JOIN DimensionesInfo d;

CREATE TABLE IF NOT EXISTS calculodimensionespais (
    calculoDimension_id SERIAL PRIMARY KEY,
    valor FLOAT,
    fecha DATE,
    flag BOOLEAN,
    pais_id INT,
    dimension_id INT,
    FOREIGN KEY (pais_id, dimension_id) REFERENCES dimensionespais(pais_id, dimension_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS calculoindicadorespais (
    calculoindicador_id SERIAL PRIMARY KEY,
    valor FLOAT,
    fecha DATE,
    flag BOOLEAN,
    dimension_id INT NOT NULL,
    pais_id INT NOT NULL,
    indicador_id INT NOT NULL,
    FOREIGN KEY (pais_id, dimension_id) REFERENCES dimensionesPais(pais_id, dimension_id) ON DELETE CASCADE,
    FOREIGN KEY (indicador_id) REFERENCES Indicadoresinfo(indicadoresinfo_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS calculobienestarpais (
    bienestar_id SERIAL PRIMARY KEY,
    pais_id INT NOT NULL,
    valor_bienestar FLOAT,
    flag BOOLEAN,
    fecha DATE, 
    FOREIGN KEY (pais_id) REFERENCES Pais(pais_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS log_archivos (
    id_log SERIAL PRIMARY KEY,
    fecha TIMESTAMP,
    nombre_archivo VARCHAR(255) NOT NULL,
    tipo_archivo VARCHAR(255) NOT NULL,
    error VARCHAR(255),
    estado VARCHAR(15)
);

