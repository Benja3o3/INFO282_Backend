"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.App = void 0;
const express_1 = __importDefault(require("express"));
const cors_1 = __importDefault(require("cors"));
const index_routes_1 = __importDefault(require("./routes/index.routes"));
const database_1 = require("./db/database");
const comunas_routes_1 = __importDefault(require("./routes/comunas.routes"));
const dimension_routes_1 = __importDefault(require("./routes/dimension.routes"));
const indicador_routes_1 = __importDefault(require("./routes/indicador.routes"));
class App {
    port;
    app;
    constructor(port) {
        this.port = port;
        this.app = (0, express_1.default)();
        this.middlewares();
        this.settings();
        this.routes();
    }
    settings() {
        this.app.set("port", process.env.PORT || this.port || 8080);
    }
    middlewares() {
        this.app.use(express_1.default.json());
        this.app.use(express_1.default.urlencoded({ extended: true }));
        this.app.use((0, cors_1.default)());
    }
    routes() {
        this.app.use(index_routes_1.default);
        this.app.use(comunas_routes_1.default);
        this.app.use(dimension_routes_1.default);
        this.app.use(indicador_routes_1.default);
    }
    async dbConnection() {
        await database_1.pool.connect();
        console.log("db connection successful");
    }
    async listen() {
        await this.app.listen(this.app.get("port"));
        console.log(`Server running on port ${this.app.get("port")}`);
    }
}
exports.App = App;
