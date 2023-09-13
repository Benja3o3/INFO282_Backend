import { Request, Response } from "express";

export function getApp(req: Request, res: Response) {
  return res.json({ message: "Hello world!" });
}
