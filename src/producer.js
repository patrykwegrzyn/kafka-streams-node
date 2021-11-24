const { Writable } = require('stream');

class Producer extends Writable {
  constructor(topic, producer) {
    super({ objectMode: true });
    this.producer = producer;
    this.topic = topic;
  }

  _write(batch, enc, callback) {
    const topic = this.topic;
    const messages = batch.length ? batch : batch.messages;
    if (messages.length === 0) {
      return callback();
    }
    this.producer
      .send({ topic, messages })
      .then((something) => {
        if (!batch.length) {
          const { highWatermark, partition } = batch;
          this.emit('produced', { partition, offset: highWatermark });
        }
        callback();
      })
      .catch((err) => callback(err));
  }
}

module.exports = Producer;
