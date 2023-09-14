import { Request, Response } from "express";
import { pool } from "../db/database";
import {
  getIndicatorsFromAllComunas,
  getOneIndicatorById,
  getAllIndicators,
} from "../db_querys/indicador.querys";

export async function getIndicadores(req: Request, res: Response) {
  const indicadores = await pool.query(getAllIndicators);
  return res.json(indicadores.rows);
}

export async function getIndicador(req: Request, res: Response) {
  const indicador = await pool.query(getOneIndicatorById(req.params.id));
  return res.json(indicador.rows);
}

export async function getIndicadoresByDimension(req: Request, res: Response) {
  console.log(req.params.id);
  const indicadores = await pool.query(
    getIndicatorsFromAllComunas(req.params.id)
  );
  return res.json(indicadores.rows);
}
