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
--CREATE INDEX idx_join ON data_subtel_antenas (comuna_id, dimension_id);


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
--CREATE INDEX idx_join ON data_cem_establecimientos (comuna_id, dimension_id);
