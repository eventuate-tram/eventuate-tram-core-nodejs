const OffsetTracker = require('./OffsetTracker');
const SqlTableBasedDuplicateMessageDetector = require('../SqlTableBasedDuplicateMessageDetector');

const { getLogger, parseMessage } = require('../utils');

const logger = getLogger({ title: 'KafkaMessageProcessor' });

class KafkaMessageProcessor {
  constructor({ subscriberId, handler }) {
    this.subscriberId = subscriberId;
    this.handler = handler;
    this.offsetTracker = new OffsetTracker();
  }

  async process(record) {
    const { topic, partition, offset } = record;
    this.offsetTracker.noteUnprocessed({ topic, partition }, offset);
    logger.debug('record: ', record);

    const { error, message } = parseMessage(record.value);

    if (error) {
      throw new Error(error);
    }

    // const sqlTableBasedDuplicateMessageDetector = new SqlTableBasedDuplicateMessageDetector(message.creationTime);
    // await sqlTableBasedDuplicateMessageDetector.doWithMessage({ subscriberId: this.subscriberId, message }, this.handler);
    await this.handler(message);

    this.offsetTracker.noteProcessed({ topic, partition }, record.offset);
    return record
  }

  noteOffsetsCommitted(offsetsToCommit) {
    this.offsetTracker.noteOffsetsCommitted(offsetsToCommit);
  }

  getPending() {
    return this.offsetTracker;
  }
}

module.exports = KafkaMessageProcessor;