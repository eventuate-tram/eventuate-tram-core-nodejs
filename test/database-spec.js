const chai = require('chai');
const { expect } = chai;
const knex = require('../lib/mysql/knex');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
const helpers = require('./lib/helpers');
const IdGenerator = require('../lib/IdGenerator');
const { makeMessageHeaders } = require('../lib/utils');
const { insertIntoMessageTable, getMessageById } = require('../lib/mysql/eventuateCommonDbOperations');

const idGenerator = new IdGenerator();
const topic = 'Database-test-topic';
const payload = 'Test';

describe('insertIntoMessageTable()', () => {
  it('should insert message', async () => {
    const messageId = await idGenerator.genIdInternal();
    const creationTime = new Date().getTime();
    const headers = makeMessageHeaders({ messageId, partitionId: 1, topic, creationTime });
    await insertIntoMessageTable(messageId, payload, topic, creationTime, headers);
    const message = await getMessageById(messageId);
    helpers.expectMessage(message, messageId, topic, payload);
  });

  it('should insert message within transaction', async () => {
    const messageId = await idGenerator.genIdInternal();
    const creationTime = new Date().getTime();
    const trx = await knex.transaction();
    const headers = makeMessageHeaders({ messageId, partitionId: 1, topic, creationTime });
    await insertIntoMessageTable(messageId, payload, topic, creationTime, headers, { trx });
    await trx.commit();
    const message = await getMessageById(messageId);
    helpers.expectMessage(message, messageId, topic, payload);
  });

  it('should not insert message if transaction canceled', async () => {
    const messageId = await idGenerator.genIdInternal();
    const creationTime = new Date().getTime();
    const trx = await knex.transaction();
    const headers = makeMessageHeaders({ messageId, partitionId: 1, topic, creationTime });
    await insertIntoMessageTable(messageId, payload, topic, creationTime, headers, { trx });
    await trx.rollback();
    const message = await getMessageById(messageId);
    expect(message).to.be.undefined;
  });
});
