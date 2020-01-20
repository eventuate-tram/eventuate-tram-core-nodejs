const SqlTableBasedDuplicateMessageDetector = require('./SqlTableBasedDuplicateMessageDetector');
const { getLogger } = require('./utils');

const logger = getLogger({ title: 'messageHandlerDecorator' });

module.exports = function (messageHandler, subscriberId) {
  return async function (message) {
    logger.debug('processing message:', message);
    const sqlTableBasedDuplicateMessageDetector = new SqlTableBasedDuplicateMessageDetector(message.creationTime);
    return sqlTableBasedDuplicateMessageDetector.doWithMessage({ subscriberId, message }, messageHandler);
  }
};