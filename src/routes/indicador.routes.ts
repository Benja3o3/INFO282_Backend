import { Router } from "express";
import {
  getIndicadores,
  getIndicador,
} from "../controllers/indicador.controller";

const router = Router();

router.get("/indicador", getIndicadores);
router.get("/indicador/:id", getIndicador);

export default router;
