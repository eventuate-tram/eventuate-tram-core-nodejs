const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
const KafkaAggregateSubscriptions = require('../lib/kafka/KafkaAggregateSubscriptions');
const IdGenerator = require('../lib/IdGenerator');
const KafkaProducer = require('../lib/kafka/KafkaProducer');
const MessageProducer = require('../lib/MessageProducer');

chai.use(chaiAsPromised);

const kafkaAggregateSubscriptions = new KafkaAggregateSubscriptions();
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
    kafkaAggregateSubscriptions.unsubscribe(),
    kafkaProducer.disconnect()
  ]);
});

describe('KafkaAggregateSubscriptions', function () {
  this.timeout(timeout);

  it('should receive Kafka message', async () => {
    const groupId = 'test-sb-id';
    return new Promise(async (resolve, reject) => {
      try {
        kafkaAggregateSubscriptions.on('message', (message) => {
          console.log('on message', message);
          resolve();
        });

        await kafkaAggregateSubscriptions.subscribe({ groupId, topics: [ topic ] });

        const messageId = await idGenerator.genIdInternal();
        const creationTime = new Date().getTime();
        const headers = messageProducer.prepareMessageHeaders(topic, { id: messageId, partitionId: 0, eventAggregateType, eventType });
        const message = JSON.stringify({
            payload: JSON.stringify({ message: 'Test kafka subscription' }),
            headers,
            offset: 5,
            partition: 0,
            highWaterOffset: 6,
            key: '0',
            timestamp: new Date(creationTime).toUTCString()
          });
        kafkaProducer.send(topic, message);
      } catch (err) {
        reject(err);
      }
    });
  });
});

