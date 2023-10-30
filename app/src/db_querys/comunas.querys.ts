export const getAllComunas = `SELECT * FROM comunas`;

export const getAllIndicatorsFromOnlyComuna = (comunaId: string) => `SELECT
i.nombre AS indicador_nombre,
d.nombre AS dimension_nombre,
ic.valor AS valor_indicador
FROM indicadoresCalculo ic
JOIN Indicadoresinfo i ON ic.indicador_id = i.indicadoresinfo_id
JOIN dimensionescomunas dc ON ic.comuna_id = dc.comuna_id AND ic.dimension_id = dc.dimension_id
JOIN Dimensiones d ON dc.dimension_id = d.dimension_id
WHERE ic.comuna_id ='${comunaId}';`;
