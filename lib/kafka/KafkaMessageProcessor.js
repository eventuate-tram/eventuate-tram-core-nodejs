const OffsetTracker = require('./OffsetTracker');

const { getLogger, parseMessage } = require('../utils');

const logger = getLogger({ title: 'KafkaMessageProcessor' });

class KafkaMessageProcessor {
  constructor({ handler }) {
    this.handler = handler;
    this.offsetTracker = new OffsetTracker();
  }

  async process(record) {
    const { topic, partition, offset } = record;
    this.offsetTracker.noteUnprocessed({ topic, partition }, offset);

    const { error, message } = parseMessage(record.value);

    if (error) {
      throw new Error(error);
    }

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