const { expect } = require('chai');
const { insertIntoMessageTable } = require('../../lib/mysql/eventuateCommonDbOperations');
const IdGenerator = require('../../lib/IdGenerator');

const idGenerator = new IdGenerator();

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

module.exports = {
  expectEnsureTopicExists,
  putMessage,
  expectEventId,
  onlyUnique
};
