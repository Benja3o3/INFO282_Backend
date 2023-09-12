import { Request, Response } from "express";
import { pool } from "../db/database";

export async function getComunas(req: Request, res: Response) {
  const comunas = await pool.query("SELECT * FROM comuna");
  console.log(comunas.rows);
  return res.json(comunas.rows);
}

export async function getComuna(req: Request, res: Response) {
  const comuna = await pool.query(
    "SELECT cut from comuna WHERE cut = " + req.params.id
  );
  return res.json(comuna.rows);
}
