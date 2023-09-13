"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getDimensionByComuna = exports.getDimension = exports.getDimensiones = void 0;
const database_1 = require("../db/database");
async function getDimensiones(req, res) {
    const dimensiones = await database_1.pool.query("SELECT * FROM dimension");
    console.log(dimensiones.rows);
    return res.json(dimensiones.rows);
}
exports.getDimensiones = getDimensiones;
async function getDimension(req, res) {
    const comuna = await database_1.pool.query("SELECT * from comuna WHERE cut = " + req.params.id);
    return res.json(comuna.rows);
}
exports.getDimension = getDimension;
async function getDimensionByComuna(req, res) {
    const dimensiones = await database_1.pool.query("SELECT * FROM dimension WHERE comuna_id = " + req.params.id);
    return res.json(dimensiones.rows);
    console.log(dimensiones.rows);
}
exports.getDimensionByComuna = getDimensionByComuna;
