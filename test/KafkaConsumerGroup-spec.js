const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
const { MessageProducer, KafkaProducer, IdGenerator, KafkaConsumerGroup } = require('../');

chai.use(chaiAsPromised);

const kafkaConsumerGroup = new KafkaConsumerGroup();
const kafkaProducer = new KafkaProducer();
const idGenerator = new IdGenerator();
const messageProducer = new MessageProducer();

const timeout = 20000;
const topic = 'test-topic';
const eventAggregateType = 'Account';
const eventType = 'charge';

before(async () => {
  await kafkaProducer.connect();
});

after(async () => {
  await Promise.all([
    kafkaConsumerGroup.unsubscribe(),
    kafkaProducer.disconnect()
  ]);
});

describe('KafkaConsumerGroup', function () {
  this.timeout(timeout);

  it('should receive Kafka message', async () => {
    const groupId = 'test-sb-id';
    return new Promise(async (resolve, reject) => {
      try {
        kafkaConsumerGroup.on('message', (message) => {
          console.log('on message', message);
          resolve();
        });

        await kafkaConsumerGroup.subscribe({ groupId, topics: [ topic ] });

        const messageId = await idGenerator.genIdInternal();
        const creationTime = new Date().toUTCString();
        const headers = messageProducer.prepareMessageHeaders(topic, { id: messageId, partitionId: 0, eventAggregateType, eventType, creationTime });
        const message = JSON.stringify({
            payload: JSON.stringify({ message: 'Test kafka subscription' }),
            headers,
            offset: 5,
            partition: 0,
            highWaterOffset: 6,
            key: '0',
            timestamp: creationTime
          });
        kafkaProducer.send(topic, message);
      } catch (err) {
        reject(err);
      }
    });
  });
});

