export const comunasData = () => `
SELECT
c.comuna_id AS cut,
cb.valor_bienestar AS bienestar,
d.nombre AS dimension,
cd.valor AS valor_dimension,
ii.nombre AS indicador,
cic.valor AS valor_indicador
FROM
Comunas c
JOIN
calculobienestarcomuna cb ON c.comuna_id = cb.comuna_id
JOIN
calculodimensionescomuna cd ON c.comuna_id = cd.comuna_id
JOIN
DimensionesInfo d ON cd.dimension_id = d.dimension_id
JOIN
calculoindicadorescomuna cic ON c.comuna_id = cic.comuna_id AND cd.dimension_id = cic.dimension_id
JOIN
Indicadoresinfo ii ON cic.indicador_id = ii.indicadoresinfo_id
ORDER BY
c.comuna_id, d.dimension_id, ii.indicadoresinfo_id;
`;

export const regionesData = () => `SELECT
r.region_id AS region,
rb.valor_bienestar AS bienestar,
d.nombre AS dimension,
rd.valor AS valor_dimension,
ii.nombre AS indicador,
ric.valor AS valor_indicador
FROM
Regiones r
JOIN
calculobienestarregion rb ON r.region_id = rb.region_id
JOIN
calculodimensionesregion rd ON r.region_id = rd.region_id
JOIN
DimensionesInfo d ON rd.dimension_id = d.dimension_id
JOIN
calculoindicadoresregion ric ON r.region_id = ric.region_id AND rd.dimension_id = ric.dimension_id
JOIN
Indicadoresinfo ii ON ric.indicador_id = ii.indicadoresinfo_id
ORDER BY
r.region_id, d.dimension_id, ii.indicadoresinfo_id;
`;

export const paisData = () => `SELECT
p.pais_id AS pais,
pb.valor_bienestar AS bienestar,
d.nombre AS dimension,
pd.valor AS valor_dimension,
ii.nombre AS indicador,
pic.valor AS valor_indicador
FROM
Pais p
JOIN
calculobienestarpais pb ON p.pais_id = pb.pais_id
JOIN
calculodimensionespais pd ON p.pais_id = pd.pais_id
JOIN
DimensionesInfo d ON pd.dimension_id = d.dimension_id
JOIN
calculoindicadorespais pic ON p.pais_id = pic.pais_id AND pd.dimension_id = pic.dimension_id
JOIN
Indicadoresinfo ii ON pic.indicador_id = ii.indicadoresinfo_id
ORDER BY
p.pais_id, d.dimension_id, ii.indicadoresinfo_id;
`;


export const indicadoresData = () => `
SELECT   indicadoresinfo.*, 
        dimensionesinfo.nombre AS dim_nombre
FROM indicadoresinfo
JOIN dimensionesinfo ON indicadoresinfo.dimension = dimensionesinfo.dimension_id`