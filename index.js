const config = require("dotenv").config();
console.log("config", config);

const operators = require("./src/operators");
const factory = require("./src/factory");

module.exports = {
  factory,
  operators,
};
