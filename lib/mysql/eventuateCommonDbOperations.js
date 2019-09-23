const { getConnection } = require('./connection');
const bPromise = require('bluebird');
const { getLogger } = require('../utils');

const logger = getLogger({ title: 'eventuateCommonDbOperations' });

const query = (sqlQuery, params, process) => bPromise.using(getConnection(), (connection) => connection
  .query(sqlQuery, params)
  .then((rows) => process(rows)));

function insertIntoMessageTable (messageId, payload, destination, currentTimeInMillisecondsSql = new Date().getTime(), headers = {}) {
  const serializedHeaders = JSON.stringify(headers);
  if (typeof (payload) === 'object') {
    payload = JSON.stringify(payload);
  }
  const sql = "INSERT INTO `message` (`id`, `destination`, `headers`, `payload`, `creation_time`) VALUES (?, ?, ?, ?, ?)";
  const params = [ messageId, destination, serializedHeaders, payload, currentTimeInMillisecondsSql ];
  logger.debug('insertIntoMessageTable', { sql, params });
  return query(sql, params, commonResult);
}

function commonResult(result) {
  return result;
}

module.exports = {
  insertIntoMessageTable
};