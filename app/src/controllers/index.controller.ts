import { Request, Response } from "express";
import { pool } from "../db/database";
import { comunasData, paisData, regionesData, indicadoresData } from "../db_querys/data.querys";

export function getApp(req: Request, res: Response) {
  return res.json({ message: "Hello world!" });
}

export async function getDataComunas(req: Request, res: Response) {
  const data = await pool.query(comunasData());
  return res.json(data.rows);
}

export async function getDataRegiones(req: Request, res: Response) {
  const data = await pool.query(regionesData());
  return res.json(data.rows);
}

export async function getDataPais(req: Request, res: Response) {
  const data = await pool.query(paisData());
  return res.json(data.rows);
}


export async function getDataIndicadores(req: Request, res: Response) {
  const data = await pool.query(indicadoresData());
  return res.json(data.rows);
}
