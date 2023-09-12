import express, { Application } from "express";
import cors from "cors";
import indexRoutes from "./routes/index.routes";
import { pool } from "./db/database";
import ComunaRoutes from "./routes/comunas.routes";
import DimensionRoutes from "./routes/dimension.routes";
import IndicadorRoutes from "./routes/indicador.routes";

export class App {
  public app: Application;

  constructor(private port?: number | string) {
    this.app = express();
    this.middlewares();
    this.settings();
    this.routes();
  }

  settings() {
    this.app.set("port", process.env.PORT || this.port || 8080);
  }

  middlewares() {
    this.app.use(express.json());
    this.app.use(express.urlencoded({ extended: true }));
    this.app.use(cors());
  }

  routes() {
    this.app.use(indexRoutes);
    this.app.use(ComunaRoutes);
    this.app.use(DimensionRoutes);
    this.app.use(IndicadorRoutes);
  }

  async dbConnection() {
    await pool.connect();
    console.log("db connection successful");
  }

  async listen() {
    await this.app.listen(this.app.get("port"));
    console.log(`Server running on port ${this.app.get("port")}`);
  }
}
