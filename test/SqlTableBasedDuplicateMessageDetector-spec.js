const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
const knex = require('../lib/mysql/knex');
const { IdGenerator, SqlTableBasedDuplicateMessageDetector } = require('../');

const idGenerator = new IdGenerator();

describe('SqlTableBasedDuplicateMessageDetector', async () => {

  describe('isDuplicate()', () => {
    let messageId;

    const consumerId = 'test-sqlTableBasedDuplicateMessageDetector';
    const currentTimeInMillisecondsSql = new Date().getTime();
    const sqlTableBasedDuplicateMessageDetector = new SqlTableBasedDuplicateMessageDetector(currentTimeInMillisecondsSql);

    before(async () => {
      messageId =  await idGenerator.genIdInternal();
    });

    it('should return true for the same message', async () => {
      const isDuplicate = await sqlTableBasedDuplicateMessageDetector.isDuplicate(consumerId, messageId);
      expect(isDuplicate).to.be.false;
    });

    it('isDuplicate() should return false', async () => {
      const isDuplicate = await sqlTableBasedDuplicateMessageDetector.isDuplicate(consumerId, messageId);
      expect(isDuplicate).to.be.true;
    });
  });
});