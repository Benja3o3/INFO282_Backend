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

INSERT INTO Dimension (id,nombre, valor, comuna_id) VALUES
    (1,'Seguridad', 70, 1),
    (2,'Educacion', 10, 1),
    (3,'Salud', 30, 1),
    (4,'Seguridad', 20, 2),
    (5,'Educacion', 19, 2),
    (6,'Salud', 96, 2),
    (7,'Seguridad', 45, 3),
    (8,'Educacion', 23, 3),
    (9,'Salud', 62, 3),
    (10,'Seguridad', 12, 4),
    (11,'Educacion', 78, 4),
    (12,'Salud', 32, 4),
    (13,'Seguridad', 15, 5),
    (14,'Educacion', 26, 5),
    (15,'Salud', 71, 5);

INSERT INTO indicador (id, nombre, prioridad, fuente, valor, dimension_id) VALUES
	(1, 'Numero de comisarias', 'Alta', 'Pagina de los carabineros', 65, 1),
	(2, 'Tasa de delitos', 'Alta', 'Pagina de los carabineros', 48, 1),
	(3, 'Tasa de denuncias', 'Alta', 'Pagina de los carabineros', 73, 1),
	(4, 'Numero de colegios', 'Alta', 'Ministerio de educacion', 65, 2),
	(5, 'Tasa de alfabetizacion', 'Alta', 'Ministerio de educacion', 42, 2),
	(6, 'Tasa de egresados', 'Alta', 'Ministerio de educacion', 27, 2),
	(7, 'Tasa de natalidad', 'Alta', 'Ministerio de salud', 79, 3),
	(8, 'Tasa de mortalidad', 'Alta', 'Ministerio de salud', 31, 3),
	(9, 'Numero de centros de salud', 'Alta', 'Ministerio de salud', 29, 3),
	(10, 'Numero de comisarias', 'Alta', 'Pagina de los carabineros', 72, 4),
	(11, 'Tasa de delitos', 'Alta', 'Pagina de los carabineros', 84, 4),
	(12, 'Tasa de denuncias', 'Alta', 'Pagina de los carabineros', 37, 4),
	(13, 'Numero de colegios', 'Alta', 'Ministerio de educacion', 91, 5),
	(14, 'Tasa de alfabetizacion', 'Alta', 'Ministerio de educacion', 53, 5),
	(15, 'Tasa de egresados', 'Alta', 'Ministerio de educacion', 65, 5),
	(16, 'Tasa de natalidad', 'Alta', 'Ministerio de salud', 42, 6),
	(17, 'Tasa de mortalidad', 'Alta', 'Ministerio de salud', 78, 6),
	(18, 'Numero de centros de salud', 'Alta', 'Ministerio de salud', 14, 6),
	(19, 'Numero de comisarias', 'Alta', 'Pagina de los carabineros', 25, 7),
	(20, 'Tasa de delitos', 'Alta', 'Pagina de los carabineros', 59, 7),
	(21, 'Tasa de denuncias', 'Alta', 'Pagina de los carabineros', 87, 7),
	(22, 'Numero de colegios', 'Alta', 'Ministerio de educacion', 31, 8),
	(23, 'Tasa de alfabetizacion', 'Alta', 'Ministerio de educacion', 79, 8),
	(24, 'Tasa de egresados', 'Alta', 'Ministerio de educacion', 50, 8),
	(25, 'Tasa de natalidad', 'Alta', 'Ministerio de salud', 23, 9),
	(26, 'Tasa de mortalidad', 'Alta', 'Ministerio de salud', 64, 9),
	(27, 'Numero de centros de salud', 'Alta', 'Ministerio de salud', 95, 9),
	(28, 'Numero de comisarias', 'Alta', 'Pagina de los carabineros', 37, 10),
	(29, 'Tasa de delitos', 'Alta', 'Pagina de los carabineros', 71, 10),
	(30, 'Tasa de denuncias', 'Alta', 'Pagina de los carabineros', 12, 10),
	(31, 'Numero de colegios', 'Alta', 'Ministerio de educacion', 45, 11),
	(32, 'Tasa de alfabetizacion', 'Alta', 'Ministerio de educacion', 88, 11),
	(33, 'Tasa de egresados', 'Alta', 'Ministerio de educacion', 19, 11),
	(34, 'Tasa de natalidad', 'Alta', 'Ministerio de salud', 52, 12),
	(35, 'Tasa de mortalidad', 'Alta', 'Ministerio de salud', 97, 12),
	(36, 'Numero de centros de salud', 'Alta', 'Ministerio de salud', 27, 12),
	(37, 'Numero de comisarias', 'Alta', 'Pagina de los carabineros', 66, 13),
	(38, 'Tasa de delitos', 'Alta', 'Pagina de los carabineros', 35, 13),
	(39, 'Tasa de denuncias', 'Alta', 'Pagina de los carabineros', 80, 13),
	(40, 'Numero de colegios', 'Alta', 'Ministerio de educacion', 57, 14),
	(41, 'Tasa de alfabetizacion', 'Alta', 'Ministerio de educacion', 22, 14),
	(42, 'Tasa de egresados', 'Alta', 'Ministerio de educacion', 76, 14),
	(43, 'Tasa de natalidad', 'Alta', 'Ministerio de salud', 40, 15),
	(44, 'Tasa de mortalidad', 'Alta', 'Ministerio de salud', 82, 15),
	(45, 'Numero de centros de salud', 'Alta', 'Ministerio de salud', 10, 15);