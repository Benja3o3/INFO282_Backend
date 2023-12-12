import { Router } from "express";
import {
  getBienestarComunas,
  getBienestarComuna,
  getDimensionesByCategoria,
  getIndicadoresByComuna,
  getDimensionesByComuna,
} from "../controllers/comunas.controller";

const router = Router();

router.get("/comunas", getBienestarComunas);
router.get("/comunas/:id", getBienestarComuna);
router.get("/comunas/dimension/categoria/:id", getDimensionesByCategoria);
router.get("/comunas/dimension/:id", getDimensionesByComuna);
router.get("/comunas/indicador/:id", getIndicadoresByComuna);



export default router;
