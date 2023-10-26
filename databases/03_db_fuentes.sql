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
CREATE INDEX idx_join ON data_subtel_antenas (comuna_id, dimension_id);
