import { Request, Response } from "express";
import { pool } from "../db/database";

export async function getIndicadores(req: Request, res: Response) {
  const indicadores = await pool.query("SELECT * FROM indicador");
  console.log(indicadores.rows);
  return res.json(indicadores.rows);
}

export async function getIndicador(req: Request, res: Response) {
  const indicador = await pool.query(
    "SELECT * from indicador WHERE cut = " + req.params.id
  );
  return res.json(indicador.rows);
}
