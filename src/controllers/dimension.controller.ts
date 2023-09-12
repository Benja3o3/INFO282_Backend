import { Request, Response } from "express";
import { pool } from "../db/database";

export async function getDimensiones(req: Request, res: Response) {
  const dimensiones = await pool.query("SELECT * FROM dimension");
  console.log(dimensiones.rows);
  return res.json(dimensiones.rows);
}

export async function getDimension(req: Request, res: Response) {
  const comuna = await pool.query(
    "SELECT cut from comuna WHERE cut = " + req.params.id
  );
  return res.json(comuna.rows);
}
