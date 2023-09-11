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
