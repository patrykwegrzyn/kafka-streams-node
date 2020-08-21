const { Kafka } = require("kafkajs");

const Producer = require("./producer");
const Consumer = require("./consumer");
const Ktable = require("./kTable");

class KafkaStreams {
  constructor(kafka) {
    this.kafka = kafka;
    this.clients = [];
  }

  async start() {
    this.admin = await this._createClient("admin");
  }

  async kTable(topic, options) {
    const consumer = await this._createClient("consumer", topic, "ktable_");
    const table = new Ktable(topic, consumer, this.admin, options);
    await table.run();

    return table;
  }

  _groupId(name) {
    return `${process.env.KAFKA_BROKER_CLIENT_ID}-${name}`;
  }

  async _createClient(type, topic, groupPrefix = "") {
    let client;
    switch (type) {
      case "consumer":
        const groupId = this._groupId(groupPrefix + topic);
        client = this.kafka.consumer({ groupId });
        break;
      default:
        client = this.kafka[type]();
    }

    this.clients.push(client);
    await client.connect();
    return client;
  }

  async producer(topic) {
    const producer = this._createClient("producer", topic);
    return new Producer(topic, producer);
  }

  async consumer(topic) {
    const consumer = await this._createClient("consumer", topic);
    await consumer.subscribe({ topic });
    return new Consumer(topic, consumer);
  }
}

function cleanup(factory) {
  const errorTypes = ["unhandledRejection", "uncaughtException"];
  const signalTraps = ["SIGTERM", "SIGINT", "SIGUSR2"];
  const cleanAll = () =>
    Promise.all(factory.clients.map((c) => c.disconnect()));

  errorTypes.map((type) => {
    process.on(type, async () => {
      try {
        console.log(`process.on ${type}`);
        await cleanAll();
        process.exit(0);
      } catch (_) {
        process.exit(1);
      }
    });
  });

  signalTraps.map((type) => {
    process.once(type, async () => {
      try {
        await cleanAll();
      } finally {
        process.kill(process.pid, type);
      }
    });
  });
}

module.exports = function (options = {}) {
  const kafka = new Kafka({
    clientId: process.env.KAFKA_BROKER_CLIENT_ID,
    brokers: process.env.KAFKA_BROKER_URI.split(","),
    ...options,
  });
  const factory = new KafkaStreams(kafka);
  cleanup(factory);
  return factory;
};
