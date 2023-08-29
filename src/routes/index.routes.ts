import { Router } from "express";
import { getApp } from "../controllers/index.controller";

const router = Router();

router.get("/", getApp);

export default router;
