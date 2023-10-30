import { Request, Response } from "express";
import { pool } from "../db/database";
import {
  getBienestarAllRegiones,
  getBienestarOneRegion,
  getDimensionesCategoria,
  getDimensionesOneRegion,
  getIndicadoresOneRegion,
} from "../db_querys/regiones.querys";

export async function getBienestarRegiones(req: Request, res: Response) {
  const bienestar = await pool.query(getBienestarAllRegiones());
  return res.json(bienestar.rows);
}
export async function getBienestarRegion(req: Request, res: Response) {
  const bienestar = await pool.query(getBienestarOneRegion(req.params.id));
  return res.json(bienestar.rows);
}

export async function getDimensionesByCategoria(req: Request, res: Response) {
  const dimensiones = await pool.query(getDimensionesCategoria(req.params.id));
  return res.json(dimensiones.rows);
}

export async function getDimensionesByRegion(req: Request, res: Response) {
  const dimensiones = await pool.query(getDimensionesOneRegion(req.params.id));
  return res.json(dimensiones.rows);
}

export async function getIndicadoresByRegion(req: Request, res: Response) {
  const indicadores = await pool.query(getIndicadoresOneRegion(req.params.id));
  return res.json(indicadores.rows);
}
