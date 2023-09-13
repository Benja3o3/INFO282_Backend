import { Router } from "express";
import { getComunas, getComuna } from "../controllers/comunas.controller";

const router = Router();

router.get("/comunas", getComunas);
router.get("/comunas/:id", getComuna);

export default router;
