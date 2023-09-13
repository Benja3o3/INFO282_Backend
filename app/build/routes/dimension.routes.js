"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = require("express");
const dimension_controller_1 = require("../controllers/dimension.controller");
const router = (0, express_1.Router)();
router.get("/dimension", dimension_controller_1.getDimensiones);
router.get("/dimension/:id", dimension_controller_1.getDimension);
router.get("/dimensionComunal/:id", dimension_controller_1.getDimensionByComuna);
exports.default = router;
