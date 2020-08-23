const { Writable } = require("stream");

class Producer extends Writable {
  constructor(topic, producer) {
    super({ objectMode: true });
    this.producer = producer;
    this.topic = topic;
  }

  _write(chunk, enc, callback) {
    this.producer
      .send({
        topic: this.topic,
        messages: chunk.length ? chunk : [chunk],
      })
      .then((something) => {
        console.log("something", something);
        this.emit("produced");
        callback();
      })
      .catch((err) => callback(err));
  }
}

module.exports = Producer;
