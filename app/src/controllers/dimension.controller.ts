import { Request, Response } from "express";
import { pool } from "../db/database";

export async function getDimensiones(req: Request, res: Response) {
  const dimensiones = await pool.query("SELECT * FROM dimension");
  console.log(dimensiones.rows);
  return res.json(dimensiones.rows);
}

export async function getDimension(req: Request, res: Response) {
  const comuna = await pool.query(
    "SELECT * from comuna WHERE cut = " + req.params.id
  );
  return res.json(comuna.rows);
}

export async function getDimensionByComuna(req: Request, res: Response) {
  const dimensiones = await pool.query(
    "SELECT * FROM dimension WHERE comuna_id = " + req.params.id
  );
  return res.json(dimensiones.rows);
  console.log(dimensiones.rows);
}
