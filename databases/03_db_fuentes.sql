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


CREATE TABLE IF NOT EXISTS data_CAB_Comisarias(
    data_id SERIAL PRIMARY KEY,
    id_comisaria INT,
    nombre_comisaria VARCHAR(255),
    direccion VARCHAR(255),
    tipo_comisaria VARCHAR(255),
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS data_SERNATUR_Turismo(
    data_id SERIAL PRIMARY KEY,
    cut_comuna_origen INT,
    cut_comuna_destino INT,
    cut_provincia_origen INT,
    cut_provincia_destino INT,
    año INT,
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

-- 9
CREATE TABLE IF NOT EXISTS data_BCN_Camaras(
    data_id SERIAL PRIMARY KEY,
    camaras FLOAT,
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);
-- 10
CREATE TABLE IF NOT EXISTS data_MINVU_Areas_Verdes(
    data_id SERIAL PRIMARY KEY,
    sup_total FLOAT,
    tipo_ep VARCHAR(255),
    shape_area FLOAT,
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);
--11
CREATE TABLE IF NOT EXISTS data_MINVU_Parques_Urbanos(
    data_id SERIAL PRIMARY KEY,
    nombre_parque VARCHAR(255),
    superficie FLOAT,
    shape_area FLOAT,
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);
-- 12
CREATE TABLE IF NOT EXISTS data_CENSO_Cantidad_Viviendas(
    data_id SERIAL PRIMARY KEY,
    total_viviendas FLOAT,
    viviendas_colectivas FLOAT,
    viviendas_moradores_presentes FLOAT,
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);

-- 13
CREATE TABLE IF NOT EXISTS data_MDFS_Pobreza_Comunal(
    data_id SERIAL PRIMARY KEY,
    numero_personas_pobreza FLOAT,
    porcentaje_personas_pobreza FLOAT,
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);


-- CREATE TABLE IF NOT EXISTS data_BCN_Tasa_Mortalidad(
--     data_id SERIAL PRIMARY KEY,
--     variable VARCHAR(255),
--     tasa_mortalidad FLOAT,
--     fecha DATE,
--     flag BOOLEAN,
--     comuna_id INT,
--     dimension_id INT,
--     FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
-- );