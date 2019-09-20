const { getConnection } = require('./connection');
const bPromise = require('bluebird');
const { getLogger } = require('../utils');

const logger = getLogger({ title: 'eventuateCommonDbOperations' });

const query = (sqlQuery, params, process) => bPromise.using(getConnection(), (connection) => connection
  .query(sqlQuery, params)
  .then((rows) => process(rows)));

const getEntityEvents = (entityId, entityTypeName) => {
  const sql = "SELECT `event_id` as id, `event_type` as eventType, `event_data` as eventData FROM `events` WHERE `entity_id` = '" + entityId + "' AND `entity_type` = '" + entityTypeName + "' ORDER BY `event_id` ASC";
  return query(sql, null, commonResult)
};

const createEntity = (entityId, entityTypeName, entityVersion, events) => {
  return new Promise((resolve, reject) => {
    bPromise.using(getConnection(), (connection) => {
      connection.beginTransaction()
        .then(insertEvents.bind(this, connection, events))
        .then(insertEntity.bind(this, connection, entityId, entityTypeName, entityVersion))
        .then(() => connection.commit())
        .then(resolve)
        .catch(err => connection.rollback()
            .then(() => reject(err))
            .catch(err => reject(err))
        );
    });
  });
};

export const updateEntity = (entityId, entityVersion, events) => {
  return new Promise((resolve, reject) => {
    bPromise.using(getConnection(), (connection) => {
      connection.beginTransaction()
        .then(insertEvents.bind(this, connection, events))
        .then(updateEntityVersion.bind(this, connection, entityId, entityVersion))
        .then(() => connection.commit())
        .then(resolve)
        .catch(err => connection.rollback()
            .then(() => reject(err))
            .catch(err => reject(err)));
    });
  });
};

function insertEntity(connection, entityId, entityTypeName, entityVersion) {
  const sql = "INSERT INTO `entities` (`entity_id`, `entity_type`, `entity_version`) VALUES (?, ?, ?)";
  return connection.query(sql, [ entityId, entityTypeName, entityVersion ]);
}

function updateEntityVersion(connection, entityId, entityVersion) {
  const sql = "UPDATE `entities` SET `entity_version` = '" + entityVersion + "' WHERE `entity_id` = ?";
  return connection.query(sql, entityId);
}

function insertEvents(connection, events) {
  const values = events.map(({ eventId, entityId, entityTypeName, eventData , eventType}) => `('${eventId}', '${entityId}', '${entityTypeName}', '${eventData}', '${eventType}')`);
  const sql = 'INSERT INTO `events` (`event_id`, `entity_id`, `entity_type`, `event_data`, `event_type`) VALUES ' + values.join(', ');
  return connection.query(sql)
}

function insertIntoMessageTable (messageId, payload, destination, currentTimeInMillisecondsSql, headers) {
  return bPromise.using(getConnection(), (connection) => {
    const serializedHeaders = JSON.stringify(headers);
    const sql = "INSERT INTO `message` (`id`, `destination`, `headers`, `payload`, `creation_time`) VALUES (?, ?, ?, ?, ?)";
    return connection.query(sql, [ messageId, destination, serializedHeaders, payload, currentTimeInMillisecondsSql ]);
  });
}

function commonResult(result) {
  return result;
}

module.exports = {
  insertIntoMessageTable
};