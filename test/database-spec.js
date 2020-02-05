const chai = require('chai');
const { expect } = chai;
const knex = require('../lib/mysql/knex');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
const helpers = require('./lib/helpers');
const { IdGenerator, MessageProducer } = require('../');
const { getMessageById } = require('../lib/mysql/eventuateCommonDbOperations');

const idGenerator = new IdGenerator();
const messageProducer = new MessageProducer();

const topic = 'Database-test-topic';
const payload = '{"text": "test database"}';
const eventAggregateType = 'Account';
const eventAggregateId = 'Fake_aggregate_id';
const eventType = 'charge';

describe('insertIntoMessageTable()', () => {
  it('should insert message', async () => {
    const messageId = await idGenerator.genIdInternal();
    const message = { payload, headers: { ID: messageId, PARTITION_ID: 0, DATE: new Date().getTime(), 'event-aggregate-id': eventAggregateId, 'event-aggregate-type': eventAggregateType, 'event-type': eventType }};
    await messageProducer.send(topic, message);
    const sentMessage = await getMessageById(messageId);
    helpers.expectDbMessage(sentMessage, messageId, topic, payload);
  });

  it('should insert message within transaction', async () => {
    const messageId = await idGenerator.genIdInternal();
    const trx = await knex.transaction();
    const message = { payload, headers: { ID: messageId, PARTITION_ID: 0, DATE: new Date().getTime(), 'event-aggregate-id': eventAggregateId, 'event-aggregate-type': eventAggregateType, 'event-type': eventType }};
    await messageProducer.send(topic, message, trx);
    await trx.commit();
    const sentMessage = await getMessageById(messageId);
    helpers.expectDbMessage(sentMessage, messageId, topic, payload);
  });

  it('should not insert message if transaction canceled', async () => {
    const messageId = await idGenerator.genIdInternal();
    const trx = await knex.transaction();
    const message = { payload, headers: { ID: messageId, PARTITION_ID: 0, DATE: new Date().getTime(), 'event-aggregate-id': eventAggregateId, 'event-aggregate-type': eventAggregateType, 'event-type': eventType }};
    await messageProducer.send(topic, message, trx);
    await trx.rollback();
    const sentMessage = await getMessageById(messageId);
    expect(sentMessage).to.be.undefined;
  });
});
