import { Router } from "express";
import {
  getDimensiones,
  getDimensionByComuna,
} from "../controllers/dimension.controller";

const router = Router();

router.get("/dimension", getDimensiones);
router.get("/dimension/:id", getDimensionByComuna);

export default router;
