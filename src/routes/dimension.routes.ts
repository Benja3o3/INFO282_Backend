import { Router } from "express";
import {
  getDimensiones,
  getDimension,
} from "../controllers/dimension.controller";

const router = Router();

router.get("/dimension", getDimensiones);
router.get("/dimension/:id", getDimension);

export default router;
