const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
const { MessageProducer, KafkaProducer, IdGenerator, MessageConsumer } = require('../');

chai.use(chaiAsPromised);

const messageConsumer = new MessageConsumer();
const kafkaProducer = new KafkaProducer();
const idGenerator = new IdGenerator();
const messageProducer = new MessageProducer();

const timeout = 20000;
const topic = 'test-topic';

before(async () => await kafkaProducer.connect());

after(async () => {
  await Promise.all([
    messageConsumer.disconnect(),
    kafkaProducer.disconnect()
  ]);
});

describe('MessageConsumer', function () {
  this.timeout(timeout);

  it('should ensureTopicExistsBeforeSubscribing()', async () => {
    const result = await messageConsumer.ensureTopicExistsBeforeSubscribing({ topics: [ topic ]});
    helpers.expectEnsureTopicExists(result);
  });

  it('should receive Kafka message', async () => {
    const subscriberId = 'test-message-consumer-sb-id';
    return new Promise(async (resolve, reject) => {

      try {
        await messageConsumer.subscribe({ subscriberId, topics: [topic], messageHandler: async message => resolve() });
      } catch (err) {
        reject(err);
      }

      try {
        const messageId = await idGenerator.genIdInternal();
        const creationTime = new Date().toUTCString();
        const message = await makeMessage(messageId, creationTime);
        await kafkaProducer.send(topic, message);
      } catch (err) {
        reject(err);
      }
    });
  });
});

async function makeMessage(messageId, creationTime) {
  const headers = await messageProducer.prepareMessageHeaders(topic, {
   headers: {
     ID: messageId,
     PARTITION_ID: 0,
     DATE: creationTime
   }}
  );
  return JSON.stringify({
    payload: JSON.stringify({ message: 'Test kafka subscription' }),
    headers,
    offset: 5,
    partition: 0,
    highWaterOffset: 6,
    key: '0',
    timestamp: creationTime
  });
}