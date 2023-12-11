export const getBienestarAllRegiones = () => `SELECT
region_id,
valor_bienestar
FROM calculobienestarregion
WHERE flag = true
ORDER BY region_id;`;

export const getBienestarOneRegion = (comunaId: string) => `SELECT
valor_bienestar
FROM calculobienestarregion
WHERE flag = true AND region_id = '${comunaId}'`;

export const getDimensionesCategoria = (categoria: string) => `SELECT
cd.region_id,
di.nombre,
cd.valor
FROM calculodimensionesregion cd
JOIN dimensionesinfo di ON cd.dimension_id = di.dimension_id
WHERE cd.flag = true AND di.nombre = '${categoria}'
ORDER BY cd.region_id;`;

export const getDimensionesOneRegion = (comunaId: string) => `SELECT
di.nombre,
cd.valor
FROM calculodimensionesregion cd
JOIN dimensionesinfo di ON cd.dimension_id = di.dimension_id
WHERE cd.flag = true AND cd.region_id = '${comunaId}'
ORDER BY di.nombre;`;

export const getIndicadoresOneRegion = (comunaId: string) => `SELECT
di.nombre as dimension,
ii.nombre as indicador,
MAX(ci.valor) as valor
FROM calculoindicadoresregion ci
JOIN dimensionesinfo di ON ci.dimension_id = di.dimension_id
JOIN indicadoresinfo ii ON ii.indicadoresinfo_id = ci.indicador_id
WHERE ci.flag = true AND ci.region_id = '${comunaId}'
GROUP BY di.nombre, ii.nombre
ORDER BY dimension, indicador`;
