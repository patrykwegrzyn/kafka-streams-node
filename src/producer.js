const { Writable } = require("stream");

class Producer extends Writable {
  constructor(topic, producer) {
    super({ objectMode: true });
    this.producer = producer;
    this.topic = topic;
  }

  _write(batch, enc, callback) {
    const { highWatermark, partition, messages } = batch;
    const topic = this.topic;
    this.producer
      .send({ topic, messages })
      .then((something) => {
        this.emit("produced", { partition, offset: highWatermark });
        callback();
      })
      .catch((err) => callback(err));
  }
}

module.exports = Producer;
