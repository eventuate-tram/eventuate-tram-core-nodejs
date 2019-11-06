const ObservableQueue = require('../ObservableQueue');
const { getLogger } = require('../utils');

class SwimlaneDispatcher {
  constructor({ messageHandlers, logger, executor = {} } = {}) {

    this.messageHandlers = messageHandlers;
    this.logger = logger || getLogger({ title: 'SwimlaneDispatcher' });
    this.executor = executor;
    this.queues = {};
  }

  dispatch(message) {
    return new Promise((resolve, reject) => {
      const { partitionId, topic } = message;
      this.logger.debug(`dispatch() topic: ${topic}, swimlane: ${partitionId}`);

      let queue = this.getQueue(topic, partitionId);

      if (!queue) {
        this.logger.debug(`Create new queue for topic: ${topic}, swimlane: ${partitionId}`);
        queue = new ObservableQueue({ topic, swimlane: partitionId, messageHandlers: this.messageHandlers, executor: this.executor });
        this.saveQueue(queue);
      }

      queue.queueMessage({ message, resolve, reject });
    });
  }

  getQueue(topic, swimlane) {
    if(!this.queues[topic]) {
      this.queues[topic] = {};
    }
    return this.queues[topic][swimlane];
  }

  saveQueue(queue) {
    const { topic, swimlane } = queue;
    this.queues[topic][swimlane] = queue;
  }
}

module.exports = SwimlaneDispatcher;
