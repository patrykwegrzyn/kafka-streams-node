const { promisify } = require("util");

const rocks = require("level-rocksdb");
const sub = require("subleveldown");
const autoIndex = require("level-auto-index");

const db = rocks(process.env.DB_PATH || "db");

class Store {
  constructor(id, indexes = []) {
    this.id = id;
    this.indexes = indexes;
    this.root = this._add(id);
    this._init();
  }

  _init() {
    if (Array.isArray(this.indexes)) {
      this.indexes.forEach((i) => this.addIndex(i));
    } else {
      Object.keys(this.indexes).forEach((name) => {
        const func = this.indexes[name];
        this.addIndex(name, func);
      });
    }
  }

  addIndex(name, keyReducer) {
    keyReducer = keyReducer || autoIndex.keyReducer(name);
    const indexId = `${this.id}-${name}`;
    console.log("keyReducer", name, keyReducer.toString());
    const index = autoIndex(this.root, this._add(indexId), keyReducer);
    index.get = promisify(index.get);
    this[`by_${name}`] = index;
  }

  put(key, value) {
    return this.root.put(key, value);
  }

  del(key) {
    this.root.del(key);
  }

  get(key) {
    return this.root.get(key, { valueEncoding: "json" });
  }

  _add(name) {
    return sub(db, name, { valueEncoding: "json" });
  }
}

module.exports = Store;
