"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getApp = void 0;
function getApp(req, res) {
    return res.json({ message: "Hello World!" });
}
exports.getApp = getApp;
