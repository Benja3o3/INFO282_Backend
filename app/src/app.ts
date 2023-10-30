import express, { Application } from "express";
import cors from "cors";
import indexRoutes from "./routes/index.routes";
import { pool } from "./db/database";
import ComunaRoutes from "./routes/comunas.routes";
import RegionesRoutes from "./routes/regiones.routes";
import PaisRoutes from "./routes/pais.routes";

export class App {
  public app: Application;

  constructor(private port?: number | string) {
    this.app = express();
    this.middlewares();
    this.settings();
    this.routes();
  }

  settings() {
    this.app.set("port", process.env.APIPORT || this.port || 5002);
  }

  middlewares() {
    this.app.use(express.json());
    this.app.use(express.urlencoded({ extended: true }));
    this.app.use(cors());
  }

  routes() {
    this.app.use(indexRoutes);
    this.app.use(ComunaRoutes);
    this.app.use(RegionesRoutes);
    this.app.use(PaisRoutes);
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
