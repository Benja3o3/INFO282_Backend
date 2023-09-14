import { Request, Response } from "express";
import { pool } from "../db/database";
import {
  getAllIndicatorsFromOnlyComuna,
  getAllComunas,
} from "../db_querys/comunas.querys";

export async function getComunas(req: Request, res: Response) {
  const comunas = await pool.query(getAllComunas);
  return res.json(comunas.rows);
}

export async function getComuna(req: Request, res: Response) {
  const comuna = await pool.query(
    getAllIndicatorsFromOnlyComuna(req.params.id)
  );
  return res.json(comuna.rows);
}
