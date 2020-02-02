const { Producer, KafkaClient } = require('kafka-node');
const isPortReachable = require('is-port-reachable');
const { ensureEnvVariable } = require('../env');

const { getLogger } = require('../utils');

const logger = getLogger({ title: 'KafkaProducer' });
const connectionString = ensureEnvVariable('EVENTUATE_TRAM_KAFKA_BOOTSTRAP_SERVERS');

class KafkaProducer {

  constructor() {
    this.connectionString = connectionString;
    this.connectionTimeout = 5000;
  }

  async connect() {
    const reachable = await this.checkKafkaIsReachable();
    logger.debug('reachable:', reachable);

    if (!reachable) {
      throw new Error(`Host unreachable ${this.connectionString}`);
    }

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

  disconnect() {
    return new Promise((resolve, reject) => {
      this.producer.close((err) => {
        if (err) {
          return reject(err);
        }

        logger.debug('disconnect() success');
        return resolve();
      });
    });
  }

  checkKafkaIsReachable() {
    const [ host, port ] = this.connectionString.split(':');
    const timeout = this.connectionTimeout;

    logger.debug(`checkKafkaIsReachable(): ${host}:${port}`);

    return isPortReachable(port, { host, timeout })
  }

  send(topic, message, partition = 0) {
    return new Promise((resolve, reject) => {
      if (typeof (message) === 'object') {
        message = JSON.stringify(message);
      }
      const payloads = [ { topic, messages: message, partition }];
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
