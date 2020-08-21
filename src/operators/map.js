const { Transform } = require("stream");
async function noop(chunk) {
  return chunk;
}

class _Map extends Transform {
  constructor(func) {
    super({ objectMode: true });
    this.func = func || noop;
  }

  async _transform(chunk, enc, callback) {
    // console.log("chunk", chunk);
    const _chunk = await this.func(chunk);
    this.push(_chunk);
    callback();
  }
}

module.exports = _Map;
