const IdGenerator = require('../lib/IdGenerator');
const { insertIntoMessageTable } = require('../lib/mysql/eventuateCommonDbOperations');

class MessageProducer {

  constructor({ channelMapping } = {}) {
    this.idGenerator = new IdGenerator();
    this.channelMapping = channelMapping;
  }

  prepareMessageHeaders(destination, message) {
    let { headers: { ID, PARTITION_ID, DATE }} = message;
    if (!ID) {
      ID = this.idGenerator.genIdInternal();
    }

    return {
      ID,
      PARTITION_ID: PARTITION_ID.toString(),
      DESTINATION: this.channelMapping ? this.channelMapping.transform(destination) : destination,
      DATE,
      'event-aggregate-type': message.headers['event-aggregate-type'],
      'event-type': message.headers['event-type']
    }
  }

  send(destination, message, trx) {
    const headers = this.prepareMessageHeaders(destination, message);
    const { payload } = message;
    const { headers: { ID: messageId, DATE: creationTime }} = message;
    return insertIntoMessageTable(messageId, payload, destination, new Date(creationTime).getTime(), headers, { trx });
  }
}

module.exports = MessageProducer;
