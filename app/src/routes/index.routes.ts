import { Router } from "express";
import {
  getApp,
  getDataComunas,
  getDataPais,
  getDataRegiones,
  getDataIndicadores,
} from "../controllers/index.controller";

const router = Router();

router.get("/", getApp);
router.get("/comunasData", getDataComunas);
router.get("/regionesData", getDataRegiones);
router.get("/paisData", getDataPais);
router.get("/indicadoresData", getDataIndicadores);

export default router;
