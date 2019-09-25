const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
const helpers = require('./lib/helpers');
const IdGenerator = require('../lib/IdGenerator');
const { insertIntoMessageTable, getMessageById } = require('../lib/mysql/eventuateCommonDbOperations');

const idGenerator = new IdGenerator();
const topic = 'Database-test-topic';

describe('insertIntoMessageTable()', () => {
  it('should insert message', async () => {
    const messageId = await idGenerator.genIdInternal();
    const payload = 'Test';
    await insertIntoMessageTable(messageId, payload, topic);
    const message = await getMessageById(messageId);
    console.log(message);
    helpers.expectMessage(message, messageId, topic, payload);
  });
});
