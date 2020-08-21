const config = require("dotenv").config();
console.log("config", config);

const factory = require("./src/factory");
module.exports = factory;
