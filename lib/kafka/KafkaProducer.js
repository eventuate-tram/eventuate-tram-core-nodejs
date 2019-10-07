const { Producer, KafkaClient } = require('kafka-node');
const isPortReachable = require('is-port-reachable');
const { getLogger } = require('../utils');

const logger = getLogger({ title: 'KafkaProducer' });

class KafkaProducer {

  constructor({ connectionString } = {}) {
    this.connectionString = connectionString || process.env.EVENTUATE_TRAM_ZOOKEEPER_CONNECTION_STRING;

    this.connectionTimeout = 5000;
  }

  async connect() {
    // const reachable = await this.checkKafkaIsReachable();
    // logger.debug('reachable:', reachable);
    //
    // if (!reachable) {
    //   throw new Error(`Host unreachable ${this.connectionString}`);
    // }

    return new Promise((resolve, reject) => {

      const client = new KafkaClient(this.connectionString);
      this.producer = new Producer(client);

      this.producer.on('ready', function () {
        logger.debug('KafkaProducer on ready');
        resolve();
      });

      this.producer.on('error', function (err) {
        logger.debug('KafkaProducer on error');
        reject(err);
      });
    });
  }

  checkKafkaIsReachable() {
    const [ host, port ] = this.connectionString.split(':');
    const timeout = this.connectionTimeout;

    logger.debug(`checkKafkaIsReachable(): ${host}:${port}`);

    return isPortReachable(port, { host, timeout })
  }

  send(topic, messages, partition = 0) {
    return new Promise((resolve, reject) => {
      const payloads = [ { topic, messages, partition }];
      this.producer.send(payloads, function (err, data) {
        if (err) {
          return reject(err);
        }
        logger.debug(data);
        resolve(data);
      });
    });
  }
}

module.exports = KafkaProducer;
