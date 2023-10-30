export const getBienestarAllRegiones = () => `SELECT
comuna_id,
valor_bienestar
FROM calculobienestarcomuna
WHERE flag = true
ORDER BY comuna_id;`;

export const getBienestarOneRegion = (comunaId: string) => `SELECT
valor_bienestar
FROM calculobienestarcomuna
WHERE flag = true AND comuna_id = '${comunaId}'`;

export const getDimensionesCategoria = (categoria: string) => `SELECT
cd.comuna_id,
di.nombre,
cd.valor
FROM calculodimensionescomuna cd
JOIN dimensionesinfo di ON cd.dimension_id = di.dimension_id
WHERE cd.flag = true AND di.nombre = '${categoria}'
ORDER BY cd.comuna_id;`;

export const getDimensionesOneRegion = (comunaId: string) => `SELECT
di.nombre,
cd.valor
FROM calculodimensionescomuna cd
JOIN dimensionesinfo di ON cd.dimension_id = di.dimension_id
WHERE cd.flag = true AND cd.comuna_id = '${comunaId}'
ORDER BY di.nombre;`;

export const getIndicadoresOneRegion = (comunaId: string) => `SELECT
di.nombre as dimension,
ii.nombre as indicador,
ci.valor
FROM calculoindicadorescomuna ci
JOIN dimensionesinfo di ON ci.dimension_id = di.dimension_id
JOIN indicadoresinfo ii ON ii.indicadoresinfo_id = ci.indicador_id
WHERE ci.flag = true AND ci.comuna_id = '${comunaId}'
ORDER BY dimension, indicador`;
