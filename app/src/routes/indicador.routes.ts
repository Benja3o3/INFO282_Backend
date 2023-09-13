import { Router } from "express";
import {
  getIndicadores,
  getIndicador,
  getIndicadoresByDimension,
} from "../controllers/indicador.controller";

const router = Router();

router.get("/indicador", getIndicadores);
router.get("/indicador/:id", getIndicador);
router.get("/indicadorCategoria/:id", getIndicadoresByDimension);

export default router;
