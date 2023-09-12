import { Request, Response } from "express";
import { pool } from "../db/database";

export async function getIndicadores(req: Request, res: Response) {
  const indicadores = await pool.query("SELECT * FROM indicador");
  console.log(indicadores.rows);
  return res.json(indicadores.rows);
}

export async function getIndicador(req: Request, res: Response) {
  const indicador = await pool.query(
    "SELECT * from indicador WHERE id = " + req.params.id
  );
  return res.json(indicador.rows);
}

export async function getIndicadoresByDimension(req: Request, res: Response) {
  console.log(req.params.id);
  const indicadores = await pool.query(
    "SELECT c.cut AS Comuna, d.nombre AS Dimension, d.valor AS Valor_Dimension, i.nombre AS Indicador, i.prioridad AS Prioridad_Indicador, i.fuente AS Fuente_Indicador, i.valor AS Valor_Indicador FROM comuna c JOIN Dimension d ON c.cut = d.comuna_id JOIN indicador i ON d.id = i.dimension_id WHERE d.nombre = '" +
      req.params.id +
      "'"
  );
  return res.json(indicadores.rows);
}
