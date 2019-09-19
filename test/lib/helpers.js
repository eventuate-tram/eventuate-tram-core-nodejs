const { expect } = require('chai');
const kafka = require('kafka-node');
const Producer = kafka.Producer;
const client = new kafka.KafkaClient({
  kafkaHost: process.env.EVENTUATELOCAL_ZOOKEEPER_CONNECTION_STRING,
  sessionTimeout: 30000
});
const producer = new Producer(client);

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

const produceKafkaMessage = (topic, message) => new Promise((resolve, reject) => {
  const payloads = [ { topic, messages: message, partition: 0 } ];
  producer.on('ready', () => {
    producer.send(payloads, (err, data) => {
      console.log(data);
      resolve();
    });
  });

  producer.on('error', (err) => {
    reject(err);
  });
});

module.exports = {
  expectEnsureTopicExists,
  produceKafkaMessage
};
