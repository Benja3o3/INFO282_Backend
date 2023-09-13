"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.pool = void 0;
const { Pool } = require("pg");
exports.pool = new Pool({
    user: "postgres",
    host: "localhost",
    database: "db_processing",
    password: "benja123",
});
