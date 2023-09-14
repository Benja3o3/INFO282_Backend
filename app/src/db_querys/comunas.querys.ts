export const getAllComunas = `SELECT * FROM comuna`;

export const getAllIndicatorsFromOnlyComuna = (comunaId: string) => `SELECT
C.CUT AS ComunaID,
D.nombre AS DimensionNombre,
D.valor AS DimensionValor,
I.nombre AS IndicadorNombre,
I.valor AS IndicadorValor
FROM Comuna AS C
LEFT JOIN Dimension AS D ON C.CUT = D.comuna_id
LEFT JOIN Indicador AS I ON D.ID = I.dimension_id
WHERE C.CUT = '${comunaId}'`;
