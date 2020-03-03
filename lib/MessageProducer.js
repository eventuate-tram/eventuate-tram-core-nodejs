const IdGenerator = require('../lib/IdGenerator');
const { insertIntoMessageTable } = require('../lib/mysql/eventuateCommonDbOperations');

class MessageProducer {

  constructor({ channelMapping } = {}) {
    this.idGenerator = new IdGenerator();
    this.channelMapping = channelMapping;
  }

  async prepareMessageHeaders(destination, message) {
    let { headers: { ID, PARTITION_ID, DATE, ...rest }} = message;

    if (typeof (PARTITION_ID) === 'undefined') {
      throw new Error('The PARTITION_ID header should be provided');
    }
    if (!ID) {
      ID = await this.idGenerator.genIdInternal();
    }

    return {
      ID,
      DATE: DATE || new Date().toGMTString(),
      PARTITION_ID: PARTITION_ID.toString(),
      DESTINATION: this.channelMapping ? this.channelMapping.transform(destination) : destination,
      ...rest,
    }
  }

  async send(destination, message, trx) {
    const headers = await this.prepareMessageHeaders(destination, message);
    const { payload } = message;
    const { ID: messageId } = headers;
    return insertIntoMessageTable(messageId, payload, destination, new Date().getTime(), headers, { trx });
  }
}

module.exports = MessageProducer;
