export const getBienestarOnePais = () => `SELECT
valor_bienestar
FROM calculobienestarcomuna
WHERE flag = true;`;

export const getDimensionesOnePais = () => `SELECT
di.nombre,
cd.valor
FROM calculodimensionescomuna cd
JOIN dimensionesinfo di ON cd.dimension_id = di.dimension_id
WHERE cd.flag = true'
ORDER BY di.nombre;`;

export const getIndicadoresOnePais = () => `SELECT
di.nombre as dimension,
ii.nombre as indicador,
ci.valor
FROM calculoindicadorescomuna ci
JOIN dimensionesinfo di ON ci.dimension_id = di.dimension_id
JOIN indicadoresinfo ii ON ii.indicadoresinfo_id = ci.indicador_id
WHERE ci.flag = true'
ORDER BY dimension, indicador`;
