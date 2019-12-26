const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
const knex = require('../lib/mysql/knex');
const SqlTableBasedDuplicateMessageDetector = require('../lib/SqlTableBasedDuplicateMessageDetector');

describe('SqlTableBasedDuplicateMessageDetector', async () => {
  it('isDuplicate() should return false', async () => {
    const transaction = knex.transaction;
    const consumerId = 'test-sqlTableBasedDuplicateMessageDetector';
    const messageId = '0000016f392561b5-1667497984000000';
    const currentTimeInMillisecondsSql = new Date().getTime();
    const sqlTableBasedDuplicateMessageDetector = new SqlTableBasedDuplicateMessageDetector(currentTimeInMillisecondsSql, transaction);

    const isDuplicate = await sqlTableBasedDuplicateMessageDetector.isDuplicate(consumerId, messageId);

    expect(isDuplicate).to.be.false;
  });
});