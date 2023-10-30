import { Router } from "express";
import {
  getBienestarRegiones,
  getBienestarRegion,
  getDimensionesByCategoria,
  getIndicadoresByRegion,
  getDimensionesByRegion,
} from "../controllers/regiones.controller";

const router = Router();

router.get("/regiones", getBienestarRegiones);
router.get("/regiones/:id", getBienestarRegion);
router.get("/regiones/dimension/categoria/:id", getDimensionesByCategoria);
router.get("/regiones/dimension/:id", getDimensionesByRegion);
router.get("/regiones/indicador/:id", getIndicadoresByRegion);

export default router;
