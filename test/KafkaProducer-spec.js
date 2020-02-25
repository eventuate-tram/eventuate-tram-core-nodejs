const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
const { KafkaProducer } = require('../');

const timeout = 20000;
const topic = 'test-producer-topic';

describe('KafkaProducer', function () {
  after(async () => {
    await producer.disconnect();
  });

  this.timeout(timeout);
  const producer = new KafkaProducer();

  it('should connect to the Kafka', async () => {
    await producer.connect();
  });

  it('should send a message', async () => {
    await producer.send(topic, 'test');
  });
});

