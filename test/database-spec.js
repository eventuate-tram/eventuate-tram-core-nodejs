const chai = require('chai');
const { expect } = chai;
const knex = require('../lib/mysql/knex');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
const helpers = require('./lib/helpers');
const IdGenerator = require('../lib/IdGenerator');
const MessageProducer = require('../lib/MessageProducer');
const { getMessageById } = require('../lib/mysql/eventuateCommonDbOperations');

const idGenerator = new IdGenerator();
const messageProducer = new MessageProducer();

const topic = 'Database-test-topic';
const payload = '{"text": "test database"}';
const eventAggregateType = 'Account';
const eventType = 'charge';

describe('insertIntoMessageTable()', () => {
  it('should insert message', async () => {
    const messageId = await idGenerator.genIdInternal();
    const creationTime = new Date().toUTCString();
    await messageProducer._send(messageId, topic, payload, creationTime, 0, eventAggregateType, eventType);
    const message = await getMessageById(messageId);
    helpers.expectDbMessage(message, messageId, topic, payload);
  });

  it('should insert message within transaction', async () => {
    const messageId = await idGenerator.genIdInternal();
    const creationTime = new Date().toUTCString();
    const trx = await knex.transaction();
    await messageProducer._send(messageId, topic, payload, creationTime, 0, eventAggregateType, eventType, trx);
    await trx.commit();
    const message = await getMessageById(messageId);
    helpers.expectDbMessage(message, messageId, topic, payload);
  });

  it('should not insert message if transaction canceled', async () => {
    const messageId = await idGenerator.genIdInternal();
    const creationTime = new Date().toUTCString();
    const trx = await knex.transaction();
    await messageProducer._send(messageId, topic, payload, creationTime, 0, eventAggregateType, eventType, trx);
    await trx.rollback();
    const message = await getMessageById(messageId);
    expect(message).to.be.undefined;
  });
});
