const log4js = require('log4js');

const getLogger = ({ logLevel, title } = {}) => {

  const logger = log4js.getLogger(title || 'Logger');

  if (!logLevel) {
    logLevel = (process.env.NODE_ENV !== 'production') ? 'DEBUG' :' ERROR';
  }

  logger.level = logLevel;

  return logger;
};

const makeEvent = eventStr => {

  try {

    const { id: eventId, eventType, entityId, entityType, eventData: eventDataStr, swimlane, eventToken } = JSON.parse(eventStr);

    const eventData = JSON.parse(eventDataStr);

    const event = {
      eventId,
      eventType,
      entityId,
      swimlane,
      eventData,
      eventToken,
      entityType,
    };

    return { event };
  } catch (error) {
    return { error };
  }
};

module.exports = {
  getLogger,
  makeEvent
};