\c db_transactional

CREATE TABLE IF NOT EXISTS data_Subtel_antenas(
    data_id SERIAL PRIMARY KEY,
    conectividad INT,
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS data_cem_establecimientos(
    data_id SERIAL PRIMARY KEY,
    nombre_establecimiento VARCHAR(255),
    dependencia INT,
    ruralidad INT,
    latitud VARCHAR(255),
    longitud VARCHAR(255),
    pago_matricula VARCHAR(255),
    pago_mensual VARCHAR(255),
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS data_ol_empleados(
    data_id SERIAL PRIMARY KEY,
    mujeres_empleadas INT,
    mujeres_desempleadas INT,
    hombres_empleados INT,
    hombres_desempleados INT,
    total_empleados INT,
    total_desempleados INT,
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS data_cem_evaluacion_docente(
    data_id SERIAL PRIMARY KEY,
    año_evaluacion INT,
    nombre_establecimiento VARCHAR(255),
    codigo_departamento_provincial INT,
    genero_docente INT,
    fecha_nacimiento_docente VARCHAR(255),
    puntaje_final FLOAT,
    escala_final VARCHAR(255),
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS data_subtel_conexion_fija_internet(
    data_id SERIAL PRIMARY KEY,
    Ene FLOAT,
    Feb FLOAT,
    Mar FLOAT,
    Abr FLOAT,
    May FLOAT,
    Jun FLOAT,
    Jul FLOAT,
    Ago FLOAT, 
    Sep FLOAT, 
    Oct FLOAT, 
    Nov FLOAT, 
    Dic FLOAT,
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS data_SISS_Agua_potable(
    data_id SERIAL PRIMARY KEY,
    variable VARCHAR(255),
    cobertura FLOAT,
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);

