const IdGenerator = require('../lib/IdGenerator');
const { insertIntoMessageTable } = require('../lib/mysql/eventuateCommonDbOperations');

class MessageProducer {

  constructor({ channelMapping }) {
    this.idGenerator = new IdGenerator();
    this.channelMapping = channelMapping;
  }

  _prepareMessageHeaders(destination, message) {

    let { id, partitionId, creationTime, eventAggregateType, eventType } = message;
    if (!id) {
      id = this.idGenerator.genIdInternal();
    }

    return {
      ID: id,
      PARTITION_ID: partitionId.toString(),
      DESTINATION: destination,
      DATE: creationTime,
      'event-aggregate-type': eventAggregateType,
      'event-type': eventType
    }
  }

  prepareMessageHeaders(destination, message) {
    let { headers: { ID, PARTITION_ID, DATE }} = message;
    if (!ID) {
      ID = this.idGenerator.genIdInternal();
    }

    return {
      ID,
      PARTITION_ID: PARTITION_ID.toString(),
      DESTINATION: this.channelMapping.transform(destination),
      DATE,
      'event-aggregate-type': message.headers['event-aggregate-type'],
      'event-type': message.headers['event-type']
    }
  }

  // TODO: update all tests to use send() and remove this
  _send(messageId, topic, payload, creationTime, partitionId, eventAggregateType, eventType, trx) {
    const headers = this._prepareMessageHeaders(topic, { id: messageId, partitionId, eventAggregateType, eventType, creationTime });
    creationTime = new Date(creationTime).getTime();
    return insertIntoMessageTable(messageId, payload, topic, creationTime, headers, { trx });
  }

  send(destination, message, trx) {
    const headers = this.prepareMessageHeaders(destination, message);
    const { payload } = message;
    const { headers: { ID: messageId, DATE: creationTime }} = message;
    return insertIntoMessageTable(messageId, payload, destination, new Date(creationTime).getTime(), headers, { trx });
  }
}

module.exports = MessageProducer;
