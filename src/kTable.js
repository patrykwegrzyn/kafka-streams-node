const Events = require("events");
const Store = require("./store");

class Ktable extends Events {
  constructor(topic, consumer, admin, options = {}) {
    super();
    this.topic = topic;
    this.store = new Store(topic, options.indexes, options.encoding);
    this.consumer = consumer;
    this.admin = admin;

    if (options.processMessage) {
      this.processMessage = this.processMessage;
    }
  }

  processMessage(message) {
    const { key: key_raw, headers, value } = message;
    const op = headers.op.toString();
    const key = key_raw.toString();
    return { key, value: JSON.parse(value), op };
  }

  async run() {
    const topic = this.topic;

    const offsets = await this.admin.fetchTopicOffsets(topic);
    const last = parseInt(offsets[0].high) - 1;
    const first = parseInt(offsets[0].low);

    await this.consumer.subscribe({ topic, fromBeginning: true });

    return new Promise(async (resolve, reject) => {
      this.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const { offset } = message;
          const { key, value, op } = this.processMessage(message);
          this.emit("operation", { op, message });
          switch (op) {
            case "DELETE":
              this.store.del(key);
              break;
            default:
              this.store.put(key, value);
              break;
          }

          if (last === parseInt(offset)) {
            return resolve();
          }
        },
      });

      this.consumer.seek({ topic, partition: 0, offset: first });
    });
  }
}

module.exports = Ktable;
