const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
const IdGenerator = require('../lib/IdGenerator');
const MessageProducer = require('../lib/MessageProducer');

chai.use(chaiAsPromised);

const idGenerator = new IdGenerator();
const messageProducer = new MessageProducer();

const timeout = 20000;
const topic = 'test-topic';
const eventAggregateType = 'Account';
const eventType = 'charge';

describe('MessageProducer', function () {
  this.timeout(timeout);

  describe('prepareMessageHeaders()', () => {
    it('should return correct headers', async () => {
      const messageId = await idGenerator.genIdInternal();
      const creationTime = new Date().toUTCString();
      const headersData = { id: messageId, partitionId: 0, eventAggregateType, eventType, creationTime };
      const headers = messageProducer._prepareMessageHeaders(topic, headersData);
      helpers.expectMessageHeaders(headers, Object.assign(headersData, {destination: topic}));
    });
  });

  it('should send Message involving CDC', async () => {
    const messageId = await idGenerator.genIdInternal();
    const creationTime = new Date().getTime();
    const payload = { message: 'Test kafka subscription' };
    await messageProducer._send(messageId, topic, payload, creationTime, 0, eventAggregateType, eventType);
  });
});