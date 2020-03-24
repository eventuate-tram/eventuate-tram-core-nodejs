const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
const { IdGenerator, MessageProducer } = require('../');
const knex = require('../lib/mysql/knex');

chai.use(chaiAsPromised);

const idGenerator = new IdGenerator();
const messageProducer = new MessageProducer();

const timeout = 20000;
const topic = 'test-topic';
describe('MessageProducer', function () {
  this.timeout(timeout);

  describe('prepareMessageHeaders()', () => {
    it('should return correct headers', async () => {
      const messageId = await idGenerator.genIdInternal();
      const creationTime = new Date().getTime();
      const headersData = {
        ID: messageId,
        PARTITION_ID: 0,
        DATE: creationTime
      };
      const headers = await messageProducer.prepareMessageHeaders(topic, { headers: headersData });
      helpers.expectMessageHeaders(headers, Object.assign(headersData, { destination: topic }));
    });
  });

  describe('send a message', () => {
    it('should send Message involving CDC', async () => {
      const payload = { message: 'Test kafka subscription' };
      const message = { payload, headers: { PARTITION_ID: 0, DATE: new Date().getTime() }};

      const trx = await knex.transaction();
      await messageProducer.send(topic, message, trx);
      await trx.commit();
    });
  });
});
