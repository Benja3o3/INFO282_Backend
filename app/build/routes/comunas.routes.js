"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = require("express");
const comunas_controller_1 = require("../controllers/comunas.controller");
const router = (0, express_1.Router)();
router.get("/comunas", comunas_controller_1.getComunas);
router.get("/comunas/:id", comunas_controller_1.getComuna);
exports.default = router;
