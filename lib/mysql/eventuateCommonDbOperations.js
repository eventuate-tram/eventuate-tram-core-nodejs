const knex = require('./knex');
const { getLogger } = require('../utils');

const logger = getLogger({ title: 'eventuateCommonDbOperations' });
const MESSAGE_TABLE = 'message';

function insertIntoMessageTable (messageId, payload, destination, currentTimeInMillisecondsSql = new Date().getTime(), headers = {}, context = {}) {
  const { trx } = context;
  const serializedHeaders = JSON.stringify(headers);
  if (typeof (payload) === 'object') {
    payload = JSON.stringify(payload);
  }
  const message = {
    id: messageId,
    destination,
    headers: serializedHeaders,
    payload,
    creation_time: currentTimeInMillisecondsSql
  };

  if (trx) {
    return knex(MESSAGE_TABLE).transacting(trx).insert(message);
  }

  return knex(MESSAGE_TABLE).insert(message);
}

async function getMessageById(id) {
  const [ message ] = await knex(MESSAGE_TABLE).where('id', id);
  return message;
}

module.exports = {
  insertIntoMessageTable,
  getMessageById
};