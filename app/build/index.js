"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const app_1 = require("./app");
function main() {
    const app = new app_1.App();
    app.dbConnection();
    app.listen();
}
main();
