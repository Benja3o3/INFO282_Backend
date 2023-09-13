import { Router } from "express";
import {
  getDimensiones,
  getDimension,
  getDimensionByComuna,
} from "../controllers/dimension.controller";

const router = Router();

router.get("/dimension", getDimensiones);
router.get("/dimension/:id", getDimension);
router.get("/dimensionComunal/:id", getDimensionByComuna);

export default router;
