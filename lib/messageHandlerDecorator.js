const SqlTableBasedDuplicateMessageDetector = require('./SqlTableBasedDuplicateMessageDetector');
const { getLogger } = require('./utils');

const logger = getLogger({ title: 'messageHandlerDecorator' });

module.exports = function (messageHandler, subscriberId) {
  return async function (message) {
    const sqlTableBasedDuplicateMessageDetector = new SqlTableBasedDuplicateMessageDetector(message.creationTime);
    return sqlTableBasedDuplicateMessageDetector.doWithMessage({ subscriberId, message }, messageHandler);
  }
};