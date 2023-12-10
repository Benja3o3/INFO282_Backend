import { Router } from "express";
import {
  getApp,
  getDataComunas,
  getDataPais,
  getDataRegiones,
} from "../controllers/index.controller";

const router = Router();

router.get("/", getApp);
router.get("/comunasData", getDataComunas);
router.get("/regionesData", getDataRegiones);
router.get("/paisData", getDataPais);

export default router;
