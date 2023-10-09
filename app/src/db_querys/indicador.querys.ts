export const getIndicatorsFromAllComunas = (
  indicadorDimension: string
) => `SELECT
c.CUT AS Comuna_CUT,
d.nombre AS Dimension,
i.nombre AS Indicador,
i.valor AS Valor
FROM Comuna c
JOIN Dimension d ON c.CUT = d.comuna_id
JOIN Indicador i ON d.ID = i.dimension_id
WHERE d.nombre = '${indicadorDimension}'
ORDER BY c.CUT, d.nombre, i.nombre;`;

export const getIndicatorByCUT = (indicatorId: string) => `SELECT
c.CUT AS Comuna_CUT,
d.nombre AS Dimension,
i.nombre AS Indicador,
i.valor AS Valor
FROM Comuna c
JOIN Dimension d ON c.CUT = d.comuna_id
JOIN Indicador i ON d.ID = i.dimension_id
WHERE c.CUT = '${indicatorId}'
ORDER BY c.CUT, d.nombre, i.nombre;`;

export const getAllIndicators = `SELECT * 
FROM indicador`;
