export const getIndicatorsFromAllComunas = (
  indicadorDimension: string
) => `SELECT 
c.cut AS Comuna, 
d.nombre AS Dimension, 
d.valor AS Valor_Dimension, 
i.nombre AS Indicador, 
i.prioridad AS Prioridad_Indicador, 
i.fuente AS Fuente_Indicador, 
i.valor AS Valor_Indicador 
FROM comuna c 
JOIN Dimension d ON c.cut = d.comuna_id 
JOIN indicador i ON d.id = i.dimension_id 
WHERE d.nombre = '${indicadorDimension}'`;

export const getOneIndicatorById = (indicatorId: string) => `SELECT * 
FROM indicador 
WHERE id = ${indicatorId}`;

export const getAllIndicators = `SELECT * 
FROM indicador`;
