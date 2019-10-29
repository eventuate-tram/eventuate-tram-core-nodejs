const IdGenerator = require('../lib/IdGenerator');
const { insertIntoMessageTable } = require('../lib/mysql/eventuateCommonDbOperations');

class MessageProducer {

  constructor() {
    this.idGenerator = new IdGenerator();
  }

  prepareMessageHeaders(destination, message) {
    console.log('messsage:', message);
    let { id, partitionId, creationTime, eventAggregateType, eventType } = message;
    if (!id) {
      id = this.idGenerator.genIdInternal();
    }

    return {
      ID: id,
      PARTITION_ID: partitionId.toString(),
      DESTINATION: destination,
      DATE: new Date(creationTime).toUTCString(),
      'event-aggregate-type': eventAggregateType,
      'event-type': eventType
    }
  }

  // send(messageId, topic, payload, creationTime, partitionId, eventAggregateType, eventType, trx) {
  send(messageId, topic, payload, creationTime, partitionId, eventAggregateType, eventType, trx) {
    const headers = this.prepareMessageHeaders(topic, { id: messageId, partitionId, eventAggregateType, eventType });
    return insertIntoMessageTable(messageId, payload, topic, creationTime, headers, { trx });
  }
}

module.exports = MessageProducer;
