"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = require("express");
const indicador_controller_1 = require("../controllers/indicador.controller");
const router = (0, express_1.Router)();
router.get("/indicador", indicador_controller_1.getIndicadores);
router.get("/indicador/:id", indicador_controller_1.getIndicador);
router.get("/indicadorCategoria/:id", indicador_controller_1.getIndicadoresByDimension);
exports.default = router;
