export const getAllDimensiones = `
SELECT
c.CUT AS Comuna_CUT,
d.nombre AS Dimension,
d.valor AS Valor
FROM Comuna c
JOIN Dimension d ON c.CUT = d.comuna_id
ORDER BY c.CUT, d.nombre;
`;

export const getDimensionesByComuna = (comunaId: string) => `
  SELECT * FROM dimension WHERE comuna_id = ${comunaId};
`;
