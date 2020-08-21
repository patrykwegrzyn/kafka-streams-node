const { Readable } = require("stream");

class Consumer extends Readable {
  constructor(topic, consumer) {
    super({ objectMode: true });
    this.consumer = consumer;
    this.topic = topic;
  }

  async run(options) {
    const that = this;
    return this.consumer.run({
      eachBatch: async ({ batch }) => {
        // console.log(batch);
        console.log(
          batch.fetchedOffset,
          batch.highWatermark,
          batch.partition,
          batch.rawMessages.length
        );
        that.push(batch);
      },
    });
  }

  _read() {}
}

module.exports = Consumer;
