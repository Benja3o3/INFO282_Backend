"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getIndicadoresByDimension = exports.getIndicador = exports.getIndicadores = void 0;
const database_1 = require("../db/database");
async function getIndicadores(req, res) {
    const indicadores = await database_1.pool.query("SELECT * FROM indicador");
    console.log(indicadores.rows);
    return res.json(indicadores.rows);
}
exports.getIndicadores = getIndicadores;
async function getIndicador(req, res) {
    const indicador = await database_1.pool.query("SELECT * from indicador WHERE id = " + req.params.id);
    return res.json(indicador.rows);
}
exports.getIndicador = getIndicador;
async function getIndicadoresByDimension(req, res) {
    console.log(req.params.id);
    const indicadores = await database_1.pool.query("SELECT c.cut AS Comuna, d.nombre AS Dimension, d.valor AS Valor_Dimension, i.nombre AS Indicador, i.prioridad AS Prioridad_Indicador, i.fuente AS Fuente_Indicador, i.valor AS Valor_Indicador FROM comuna c JOIN Dimension d ON c.cut = d.comuna_id JOIN indicador i ON d.id = i.dimension_id WHERE d.nombre = '" +
        req.params.id +
        "'");
    return res.json(indicadores.rows);
}
exports.getIndicadoresByDimension = getIndicadoresByDimension;
