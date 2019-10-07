const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
const KafkaProducer = require('../lib/kafka/KafkaProducer');

const timeout = 5000000;
const topic = 'test-producer-topic';

describe('KafkaProducer', function () {
  this.timeout(timeout);
  const producer = new KafkaProducer();

  it('should connect to the Kafka', async () => {
    await producer.connect();
  });

  it('should send a message', async () => {
    await producer.send(topic, 'test');
  });
});

