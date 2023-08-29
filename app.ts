import express, { Application } from "express";
import cors from "cors";

export class App {
  public app: Application;

  constructor(private port?: number | string) {
    this.app = express();
    this.middlewares();
    this.settings();
  }

  settings() {
    this.app.set("port", process.env.PORT || this.port || 8080);
  }

  middlewares() {
    this.app.use(express.json());
    this.app.use(express.urlencoded({ extended: true }));
    this.app.use(cors());
  }

  async listen() {
    await this.app.listen(this.app.get("port"));
    console.log(`Server running on port ${this.app.get("port")}`);
  }
}
