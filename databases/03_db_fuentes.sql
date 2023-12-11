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

-- 14
CREATE TABLE IF NOT EXISTS data_BCN_Rendimiento_academico(
    data_id SERIAL PRIMARY KEY,
    retirados_hombres FLOAT,
    retirados_mujeres FLOAT,
    retirados_total FLOAT,
    aprobados_hombre FLOAT,
    aprobados_mujeres FLOAT,
    aprobados_total FLOAT,
    reprobados_hombres FLOAT,
    reprobados_mujeres FLOAT,
    reprobados_total FLOAT,
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);

--17
CREATE TABLE IF NOT EXISTS data_BCN_Docentes(
    data_id SERIAL PRIMARY KEY,
    total_docentes FLOAT,
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);


--18 --19
CREATE TABLE IF NOT EXISTS data_BCN_Mortalidad(
    data_id SERIAL PRIMARY KEY,
    mortalidad_general FLOAT,
    mortalidad_hombre FLOAT,
    mortalidad_mujer FLOAT,
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);

--20
CREATE TABLE IF NOT EXISTS data_BCN_TV_Pago(
    data_id SERIAL PRIMARY KEY,
    cantidad_suscriptores FLOAT,
    periodo VARCHAR(255),
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);

--21
CREATE TABLE IF NOT EXISTS data_BCN_Tasa_de_denuncias_social(
    data_id SERIAL PRIMARY KEY,
    cantidad_denuncias FLOAT,
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);

--22
CREATE TABLE IF NOT EXISTS data_BCN_Vulnerabilidad_socio_delictual(
    data_id SERIAL PRIMARY KEY,
    indice_vulnerabilidad FLOAT,
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);

-- 22 Densidad poblacion

--23 Indice hacinamiento

CREATE TABLE IF NOT EXISTS data_BCN_Indice_hacinamiento(
    data_id SERIAL PRIMARY KEY,
    hacinamiento_critico FLOAT,
    hacinamiento_medio FLOAT,
    hacinamiento_ignorado FLOAT,
    sin_hacinamiento FLOAT,
    total_viviendas FLOAT,
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);

--24 Deficit habitacional
CREATE TABLE IF NOT EXISTS data_BCN_Deficit_habitacional(
    data_id SERIAL PRIMARY KEY,
    deficit_total FLOAT,
    hogares_allegados FLOAT,
    hogares_total FLOAT,
    nucleos_allegados FLOAT,
    viviendas_irrecuperables FLOAT,
    viviendas_totales FLOAT,    
    fecha DATE,
    flag BOOLEAN,
    comuna_id INT,
    dimension_id INT,
    FOREIGN KEY(comuna_id, dimension_id) REFERENCES comunasdimensiones(comuna_id, dimension_id) ON DELETE CASCADE
);

--25 SINIA Residuos Peligrosos
CREATE TABLE IF NOT EXISTS data_SINIA_Residuos(
    data_id SERIAL PRIMARY KEY,
    razon_soc VARCHAR(255),
    nombre_est VARCHAR(255),
    actividad VARCHAR(255),
    coordenada_1 FLOAT,
    coordenada_2 FLOAT,
    contaminante VARCHAR(255),
    peligro_id VARCHAR(255),
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