const { Readable } = require("stream");

class Consumer extends Readable {
  constructor(topic, consumer) {
    super({ objectMode: true });
    this.consumer = consumer;
    this.topic = topic;
  }

  commitOffsets(offsets) {
    this.consumer.commitOffsets(offsets.length ? offsets : [offsets]);
  }

  async run(options) {
    const that = this;
    return this.consumer.run({
      ...options,
      eachBatch: async ({ batch, isStale }) => {
        console.log("isStale", isStale());
        that.push(batch);
      },
    });
  }

  _read() {}
}

module.exports = Consumer;
