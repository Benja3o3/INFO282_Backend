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


CREATE TABLE IF NOT EXISTS Dimension (
    ID int PRIMARY KEY,
    nombre VARCHAR(255),
    valor INT,
    comuna_id int NOT NULL,
    FOREIGN KEY (comuna_id) REFERENCES Comuna(CUT) ON DELETE CASCADE

);


CREATE TABLE IF NOT EXISTS Indicador (
    ID int PRIMARY KEY,
    nombre VARCHAR(255),
    prioridad VARCHAR(255),
    fuente VARCHAR(255),
    valor INT,
    dimension_id INT NOT NULL,
    FOREIGN KEY (dimension_id) REFERENCES Dimension(ID) ON DELETE CASCADE

);

CREATE TABLE tempComuna (data jsonb);
COPY tempComuna (data) FROM '/docker-entrypoint-initdb.d/jsonFiles/comunasDB.json';

INSERT INTO Comuna
SELECT (data->>'CUT')::INT
FROM tempComuna;


-- -- Insert data into the Dimension table
-- INSERT INTO Dimension (ID, nombre, valor, comuna_id) VALUES
--     (1, 'Seguridad', 70, 1),
--     (2, 'Educacion', 10, 1),
--     (3, 'Salud', 30, 1),
--     (4, 'Seguridad', 20, 2),
--     (5, 'Educacion', 19, 2),
--     (6, 'Salud', 96, 2),
--     (7, 'Seguridad', 45, 3),
--     (8, 'Educacion', 23, 3),
--     (9, 'Salud', 62, 3),
--     (10, 'Seguridad', 12, 4),
--     (11, 'Educacion', 78, 4),
--     (12, 'Salud', 32, 4),
--     (13, 'Seguridad', 15, 5),
--     (14, 'Educacion', 26, 5),
--     (15, 'Salud', 71, 5);

-- -- Insert data into the Indicador table
-- INSERT INTO Indicador (ID, nombre, prioridad, fuente, valor, dimension_id) VALUES
--     (1, 'Indicador A', 'Alta', 'Fuente 1', 50, 1),
--     (2, 'Indicador B', 'Media', 'Fuente 2', 60, 1),
--     (3, 'Indicador C', 'Baja', 'Fuente 3', 40, 2),
--     (4, 'Indicador D', 'Alta', 'Fuente 4', 70, 2),
--     (5, 'Indicador E', 'Media', 'Fuente 5', 55, 3),
--     (6, 'Indicador F', 'Baja', 'Fuente 6', 45, 3),
--     (7, 'Indicador G', 'Alta', 'Fuente 7', 80, 4),
--     (8, 'Indicador H', 'Media', 'Fuente 8', 65, 4),
--     (9, 'Indicador I', 'Baja', 'Fuente 9', 35, 5),
--     (10, 'Indicador J', 'Alta', 'Fuente 10', 75, 5);


