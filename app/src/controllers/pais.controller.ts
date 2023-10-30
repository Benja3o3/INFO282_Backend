import { Request, Response } from "express";
import { pool } from "../db/database";
import {
  getBienestarOnePais,
  getDimensionesOnePais,
  getIndicadoresOnePais,
} from "../db_querys/pais.querys";

export async function getBienestarPais(req: Request, res: Response) {
  const bienestar = await pool.query(getBienestarOnePais());
  return res.json(bienestar.rows);
}
export async function getDimensionesPais(req: Request, res: Response) {
  const dimensiones = await pool.query(getDimensionesOnePais());
  return res.json(dimensiones.rows);
}

export async function getIndicadoresPais(req: Request, res: Response) {
  const indicadores = await pool.query(getIndicadoresOnePais());
  return res.json(indicadores.rows);
}
