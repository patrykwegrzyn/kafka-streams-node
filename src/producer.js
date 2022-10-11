const { Writable } = require("stream");

class Producer extends Writable {
  constructor(topic, producer, options = {}) {
    super({ objectMode: true });
    this.producer = producer;
    this.topic = topic;
    this.options = options;
  }

  _write(batch, enc, callback) {
    const topic = this.topic;
    const messages = batch.length ? batch : batch.messages;
    if (!messages || messages.length === 0) {
      return callback();
    }
    this.producer
      .send({ topic, messages, ...this.options })
      .then((something) => {
        if (!batch.length) {
          const { highWatermark, partition } = batch;
          this.emit("produced", { partition, offset: highWatermark });
        }
        callback();
      })
      .catch((err) => callback(err));
  }
}

module.exports = Producer;
