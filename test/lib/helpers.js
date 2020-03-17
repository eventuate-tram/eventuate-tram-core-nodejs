const { expect } = require('chai');
const { insertIntoMessageTable } = require('../../lib/mysql/eventuateCommonDbOperations');
const MessageProducer = require('../../lib/MessageProducer');
const IdGenerator = require('../../lib/IdGenerator');
const DefaultChannelMapping = require('../../lib/DefaultChannelMapping');
const { AGGREGATE_TYPE, EVENT_TYPE, AGGREGATE_ID } = require('../../lib/eventMessageHeaders');
const randomInt = require('random-int');

const idGenerator = new IdGenerator();
const channelMapping = new DefaultChannelMapping(new Map());
const messageProducer = new MessageProducer({ channelMapping });

const expectEnsureTopicExists = (res) => {
  expect(res).to.be.an('Array');
  expect(res).lengthOf(2);
  const [ nodeInfo, metadataObj ] = res;
  expect(nodeInfo).to.be.an('Object');

  expect(nodeInfo).to.haveOwnProperty('0');

  expect(nodeInfo['0']).to.be.an('Object');
  expect(nodeInfo['0']).to.haveOwnProperty('nodeId');
  expect(nodeInfo['0']).to.haveOwnProperty('host');
  expect(nodeInfo['0']).to.haveOwnProperty('port');

  expect(metadataObj).to.haveOwnProperty('metadata');
  expect(metadataObj['metadata']).to.be.an('Object');
};

const putMessage = async (topic, payload, messageId) => {
  if (!messageId) {
    messageId = await idGenerator.genIdInternal();
  }
  return insertIntoMessageTable(messageId, payload, topic);
};

const expectEventId = (eventId) => {
  expect(eventId).to.be.a('String');
  expect(eventId).to.match(/^[0-9A-z]{16}-[0-9A-z]{16}$/);
};

const onlyUnique = (value, index, self) => {
  return self.indexOf(value) === index;
};

const expectDbMessage = (message, messageId, topic, payload) => {
  expect(message).to.be.an('Object');
  expect(message).to.haveOwnProperty('id');
  expect(message.id).eq(messageId);
  expect(message).to.haveOwnProperty('destination');
  expect(message.destination).eq(topic);
  expect(message).to.haveOwnProperty('headers');
  expect(message).to.haveOwnProperty('payload');
  expect(message.payload).eq(payload);
  expect(message).to.haveOwnProperty('published');
  expect(message).to.haveOwnProperty('creation_time');

  try {
    expectMessageHeaders(JSON.parse(message.headers));
  } catch (err) {
    throw err;
  }
};

const expectMessageHeaders = (headers, headersData) => {
  expect(headers).to.haveOwnProperty('ID');
  expect(headers.ID).to.be.a('String');

  expect(headers).to.haveOwnProperty('PARTITION_ID');
  expect(headers.PARTITION_ID).to.be.a('String');

  expect(headers).to.haveOwnProperty('DESTINATION');
  expect(headers.DESTINATION).to.be.a('String');

  expect(headers).to.haveOwnProperty('DATE');

  if (headersData) {
    expect(headers.ID).eq(headersData.ID);
    expect(headers.DATE).eq(headersData.DATE);
    expect(headers.PARTITION_ID).eq(headersData.PARTITION_ID.toString());
    expect(headers.DESTINATION).eq(headersData.destination);
  }
};

const expectKafkaMessage = (message) => {
  expect(message).to.haveOwnProperty('topic');
  expect(message.topic).to.be.a('String');
  expect(message).to.haveOwnProperty('offset');
  expect(message.offset).to.be.a('Number');
  expect(message).to.haveOwnProperty('partition');
  expect(message.partition).to.be.a('Number');
  expect(message).to.haveOwnProperty('highWaterOffset');
  expect(message.highWaterOffset).to.be.a('Number');
  expect(message).to.haveOwnProperty('key');
  expect(message).to.haveOwnProperty('timestamp');
  expect(message.timestamp).to.be.a('Date');

  expect(message).to.haveOwnProperty('value');
  expect(message.value).to.be.a('String');

  try {
    const parsedValue = JSON.parse(message.value);
    expect(parsedValue).to.haveOwnProperty('payload');
    expect(parsedValue).to.haveOwnProperty('headers');
    expectMessageHeaders(parsedValue.headers);
  } catch (err) {
    throw err;
  }
};

const expectMessageForDomainEvent = (message, payload) => {
  expect(message).to.haveOwnProperty('payload');
  expect(message.payload).to.be.a('String');
  if (typeof (payload === 'object')) {
    payload = JSON.stringify(payload);
  }
  expect(message.payload).eq(payload);

  expect(message).to.haveOwnProperty('headers');

  const headers = message.headers;

  expect(headers).to.haveOwnProperty('PARTITION_ID');
  expect(headers.PARTITION_ID).to.be.a('String');

  expect(headers).to.haveOwnProperty(AGGREGATE_TYPE);
  expect(headers[AGGREGATE_TYPE]).to.be.a('String');

  expect(headers).to.haveOwnProperty(AGGREGATE_ID);
  expect(headers[AGGREGATE_ID]).to.be.a('String');

  expect(headers).to.haveOwnProperty(EVENT_TYPE);
  expect(headers[EVENT_TYPE]).to.be.a('String');
};

const expectDomainEvent = (event, payload) => {
  expect(event).to.haveOwnProperty('payload');
  expect(event.payload).to.be.a('String');

  if (typeof (payload === 'object')) {
    payload = JSON.stringify(payload);
  }
  expect(event.payload).eq(payload);

  expect(event).to.haveOwnProperty('partitionId');
  expect(event.partitionId).to.be.a('String');

  expect(event).to.haveOwnProperty(AGGREGATE_TYPE);
  expect(event[AGGREGATE_TYPE]).to.be.a('String');

  expect(event).to.haveOwnProperty(AGGREGATE_ID);
  expect(event[AGGREGATE_ID]).to.be.a('String');

  expect(event).to.haveOwnProperty(EVENT_TYPE);
  expect(event[EVENT_TYPE]).to.be.a('String');

  expect(event).to.haveOwnProperty('creationTime');
  expect(event.creationTime).to.be.a('String');
};

const fakeKafkaMessage = async ({ topic, eventAggregateType, eventType, partition = 0, payload }) => {
  const creationTime = new Date().getTime();

  const headers = await messageProducer.prepareMessageHeaders(topic, {
    headers: {
      PARTITION_ID: partition,
      [AGGREGATE_TYPE]: eventAggregateType,
      [EVENT_TYPE]: eventType,
      DATE: creationTime
    }
  });

  return {
    payload: payload || 'Fake message',
    headers,
    offset: 5,
    partition,
    highWaterOffset: 6,
    key: '0',
    timestamp: creationTime
  };
};

const sleep = timeout => new Promise((resolve, reject) => setTimeout(() => resolve(), timeout));

async function randomSleep() {
  const timeout = randomInt(100, 1000);
  console.log('sleep ' + timeout);
  await sleep(timeout);
}

function sequenceKafkaSend(promise, message) {
  return new Promise((resolve) => {
    resolve(promise.then(_ => kafkaProducer.send(topic, message, message.partition)));
  });
}

module.exports = {
  expectEnsureTopicExists,
  putMessage,
  expectEventId,
  onlyUnique,
  expectDbMessage,
  expectMessageHeaders,
  expectKafkaMessage,
  expectMessageForDomainEvent,
  fakeKafkaMessage,
  sleep,
  randomSleep,
  expectDomainEvent,
};
