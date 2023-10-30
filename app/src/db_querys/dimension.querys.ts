export const getAllDimensiones = `
SELECT
cd.comuna_id,
d.nombre,
cd.valor
FROM calculodimensiones cd
JOIN dimensiones d ON cd.dimension_id = d.dimension_id
WHERE cd.flag = true;
`;

export const getDimensionesByComuna = (comunaId: string) => `
SELECT
cd.comuna_id,
d.nombre,
cd.valor
FROM calculodimensiones cd
JOIN dimensiones d ON cd.dimension_id = d.dimension_id
WHERE cd.comuna_id = '${comunaId}' AND 
cd.flag = true;
`;
