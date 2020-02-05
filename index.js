const DomainEventPublisher = require('./lib/DomainEventPublisher');
const DomainEventDispatcher = require('./lib/DomainEventDispatcher');
const DefaultChannelMapping = require('./lib/DefaultChannelMapping');
const DefaultDomainEventNameMapping = require('./lib/DefaultDomainEventNameMapping');
const eventMessageHeaders = require('./lib/eventMessageHeaders');
const KafkaConsumerGroup = require('./lib/kafka/KafkaConsumerGroup');
const IdGenerator = require('./lib/IdGenerator');
const MessageProducer = require('./lib/MessageProducer');
const SqlTableBasedDuplicateMessageDetector = require('./lib/SqlTableBasedDuplicateMessageDetector');
const MessageConsumer = require('./lib/kafka/MessageConsumer');
const SwimlaneDispatcher = require('./lib/kafka/SwimlaneDispatcher');
const ObservableQueue = require('./lib/ObservableQueue');
const KafkaProducer = require('./lib/kafka/KafkaProducer');

module.exports = {
  KafkaProducer,
  KafkaConsumerGroup,
  SqlTableBasedDuplicateMessageDetector,
  MessageConsumer,
  MessageProducer,
  eventMessageHeaders,
  IdGenerator,
  SwimlaneDispatcher,
  ObservableQueue,
  DomainEventPublisher,
  DomainEventDispatcher,
  DefaultChannelMapping,
  DefaultDomainEventNameMapping
};