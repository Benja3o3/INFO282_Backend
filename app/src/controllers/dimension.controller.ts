import { Request, Response } from "express";
import { pool } from "../db/database";
import {
  getAllDimensiones,
  getDimensionesByComuna,
} from "../db_querys/dimension.querys";

export async function getDimensiones(req: Request, res: Response) {
  const dimensiones = await pool.query(getAllDimensiones);
  return res.json(dimensiones.rows);
}

export async function getDimensionByComuna(req: Request, res: Response) {
  const dimensiones = await pool.query(getDimensionesByComuna(req.params.id));
  return res.json(dimensiones.rows);
}
