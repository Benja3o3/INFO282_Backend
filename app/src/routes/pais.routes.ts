import { Router } from "express";
import {
  getBienestarPais,
  getDimensionesPais,
  getIndicadoresPais,
} from "../controllers/pais.controller";

const router = Router();

router.get("/pais", getBienestarPais);
router.get("/pais/dimension", getDimensionesPais);
router.get("/pais/indicador", getIndicadoresPais);

export default router;
