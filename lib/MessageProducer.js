const IdGenerator = require('../lib/IdGenerator');
const { insertIntoMessageTable } = require('../lib/mysql/eventuateCommonDbOperations');
const { AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE } = require('./eventMessageHeaders');

class MessageProducer {

  constructor({ channelMapping } = {}) {
    this.idGenerator = new IdGenerator();
    this.channelMapping = channelMapping;
  }

  async prepareMessageHeaders(destination, message) {
    let { headers: { ID, PARTITION_ID, DATE, AGGREGATE_ID }} = message;
    if (!ID) {
      ID = await this.idGenerator.genIdInternal();
    }
    if (!AGGREGATE_ID) {
      AGGREGATE_ID = await this.idGenerator.genIdInternal();
    }

    return {
      ID,
      DATE,
      PARTITION_ID: PARTITION_ID.toString(),
      DESTINATION: this.channelMapping ? this.channelMapping.transform(destination) : destination,
      AGGREGATE_TYPE: message.headers[AGGREGATE_TYPE],
      EVENT_TYPE: message.headers[EVENT_TYPE],
      AGGREGATE_ID
    }
  }

  async send(destination, message, trx) {
    const headers = await this.prepareMessageHeaders(destination, message);
    const { payload } = message;
    const { ID: messageId } = headers;
    return insertIntoMessageTable(messageId, payload, destination, headers, { trx });
  }
}

module.exports = MessageProducer;
