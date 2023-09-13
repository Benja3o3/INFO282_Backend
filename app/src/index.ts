import { App } from "./app";

function main() {
  const app = new App();
  app.dbConnection();
  app.listen();
}

main();
