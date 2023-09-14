export const getAllDimensiones = `
  SELECT * FROM dimension;
`;

export const getDimensionesByComuna = (comunaId: string) => `
  SELECT * FROM dimension WHERE comuna_id = ${comunaId};
`;
