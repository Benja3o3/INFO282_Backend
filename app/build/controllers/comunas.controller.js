"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getComuna = exports.getComunas = void 0;
const database_1 = require("../db/database");
async function getComunas(req, res) {
    const comunas = await database_1.pool.query("SELECT * FROM comuna");
    console.log(comunas.rows);
    return res.json(comunas.rows);
}
exports.getComunas = getComunas;
async function getComuna(req, res) {
    const comuna = await database_1.pool.query("SELECT cut from comuna WHERE cut = " + req.params.id);
    return res.json(comuna.rows);
}
exports.getComuna = getComuna;
