const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
const { IdGenerator, MessageProducer } = require('../');

chai.use(chaiAsPromised);

const idGenerator = new IdGenerator();
const messageProducer = new MessageProducer();

const timeout = 20000;
const topic = 'test-topic';
const eventAggregateType = 'Account';
const eventType = 'charge';
const eventAggregateId = 'Fake_aggregate_id';

describe('MessageProducer', function () {
  this.timeout(timeout);

  describe('prepareMessageHeaders()', () => {
    it('should return correct headers', async () => {
      const messageId = await idGenerator.genIdInternal();
      const creationTime = new Date().getTime();
      const headersData = { ID: messageId, PARTITION_ID: 0, 'event-aggregate-type': eventAggregateType, 'event-type': eventType, DATE: creationTime };
      const headers = await messageProducer.prepareMessageHeaders(topic, { headers: headersData });
      helpers.expectMessageHeaders(headers, Object.assign(headersData, {destination: topic}));
    });
  });

  it('should send Message involving CDC', async () => {
    const messageId = await idGenerator.genIdInternal();
    const payload = { message: 'Test kafka subscription' };
    const message = { payload, headers: { ID: messageId, PARTITION_ID: 0, DATE: new Date().getTime(), 'event-aggregate-id': eventAggregateId, 'event-aggregate-type': eventAggregateType, 'event-type': eventType }};
    await messageProducer.send(topic, message);
  });
});