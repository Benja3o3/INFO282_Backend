CREATE TABLE IF NOT EXISTS Comuna (
    CUT int PRIMARY KEY,
    Nombre VARCHAR(255),
    Poblacion int,
    Geometria GEOMETRY(Point, 4326)
);
