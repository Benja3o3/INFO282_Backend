import { Request, Response } from "express";
import { pool } from "../db/database";
import {
  getBienestarAllComunas,
  getBienestarOneComuna,
  getDimensionesCategoria,
  getDimensionesOneComuna,
  getIndicadoresOneComuna,
} from "../db_querys/comunas.querys";

export async function getBienestarComunas(req: Request, res: Response) {
  const bienestar = await pool.query(getBienestarAllComunas());
  return res.json(bienestar.rows);
}
export async function getBienestarComuna(req: Request, res: Response) {
  const bienestar = await pool.query(getBienestarOneComuna(req.params.id));
  return res.json(bienestar.rows);
}

export async function getDimensionesByCategoria(req: Request, res: Response) {
  const dimensiones = await pool.query(getDimensionesCategoria(req.params.id));
  return res.json(dimensiones.rows);
}

export async function getDimensionesByComuna(req: Request, res: Response) {
  const dimensiones = await pool.query(getDimensionesOneComuna(req.params.id));
  return res.json(dimensiones.rows);
}

export async function getIndicadoresByComuna(req: Request, res: Response) {
  const indicadores = await pool.query(getIndicadoresOneComuna(req.params.id));
  return res.json(indicadores.rows);
}

