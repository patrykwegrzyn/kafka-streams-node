const { Writable } = require("stream");

class Producer extends Writable {
  constructor(producer) {
    super({ objectMode: true });
    this.producer = producer;
  }

  _write(options, enc, callback) {
    this.producer
      .send(options)
      .then((something) => {
        // this.emit("produced", { partition, offset: highWatermark });
        callback();
      })
      .catch((err) => callback(err));
  }
}

module.exports = Producer;
