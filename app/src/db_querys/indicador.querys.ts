export const getIndicatorsFromAllComunas = (
  indicadorDimension: string
) => `SELECT
ic.comuna_id AS cut,
d.nombre AS dimension,
ii.nombre AS indicador,
ic.valor
FROM indicadorescalculo ic
JOIN indicadoresinfo ii ON ic.indicador_id = ii.indicadoresinfo_id
JOIN dimensiones d ON ic.dimension_id = d.dimension_id
WHERE d.nombre = '${indicadorDimension}' AND ic.flag = true
ORDER BY ic.comuna_id, d.nombre, ii.nombre;`;

export const getIndicatorByCUT = (indicatorId: string) => `SELECT
d.nombre AS dimension,
ii.nombre AS indicador,
ic.valor
FROM indicadorescalculo ic
JOIN indicadoresinfo ii ON ic.indicador_id = ii.indicadoresinfo_id
JOIN dimensiones d ON ic.dimension_id = d.dimension_id
WHERE ic.comuna_id = '${indicatorId}' AND ic.flag = true
ORDER BY d.nombre, ii.nombre;`;

export const getAllIndicators = `SELECT * 
FROM indicadorescalculo`;
