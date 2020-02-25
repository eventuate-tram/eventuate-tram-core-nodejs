const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
const { KafkaProducer, ObservableQueue, SwimlaneDispatcher, MessageConsumer } = require('../');

chai.use(chaiAsPromised);

const messageConsumer = new MessageConsumer();
const kafkaProducer = new KafkaProducer();

const timeout = 25000;
const topic = 'test-topic-swimlane-dispatcher';
const eventAggregateType = 'Account';
const eventType = 'CreditApproved';
const expectedProcessedMessagesNum = 6;

before(async () => await kafkaProducer.connect());

after(async () => {
  await Promise.all([
    messageConsumer.disconnect(),
    kafkaProducer.disconnect()
  ]);
});

const messagesBySwimlane = {
  0: [ 100, 5, 10 ],
  1: [ 14, 6, 103 ]
};

const resultsBySwimlane = { 0: [], 1: [] };

describe('SwimlaneDispatcher', function () {
  this.timeout(timeout);

  let swimlaneDispatcher;

  it('should dispatch a message', async () => {
    const subscriberId = 'test-swimlane-dispatcher-id';

    return new Promise(async (resolve, reject) => {
      let processedNum = 0;

      const messageHandlers = {
        [topic]: async (message) => {
          console.log('Processing queue message:', message);
          await helpers.randomSleep();
          pushMessageToResults(message);

          processedNum++;
          if (processedNum >= expectedProcessedMessagesNum) {
            console.log('resultsBySwimlane', resultsBySwimlane);
            console.log('messagesBySwimlane', messagesBySwimlane);
            expect(resultsBySwimlane).to.deep.eq(messagesBySwimlane);
            resolve();
          }
        }
      };

      swimlaneDispatcher = new SwimlaneDispatcher({ messageHandlers });
      const messageHandler = (message) => {
        return swimlaneDispatcher.dispatch(message);
      };

      try {
        await messageConsumer.subscribe({ subscriberId, topics: [ topic ], messageHandler });
        const messages = await generateMessages();
        await sendMessages(messages);
      } catch (err) {
        reject(err);
      }
    });
  });

  it('should have queues for each swimlane', () => {
    expect(swimlaneDispatcher).to.be.ok;
    expect(swimlaneDispatcher).to.haveOwnProperty('queues');
    expect(swimlaneDispatcher.queues).to.haveOwnProperty(topic);
    Object.keys(messagesBySwimlane).forEach((swimlane) => {
      expect(swimlaneDispatcher.queues[topic]).to.haveOwnProperty(swimlane);
      expect(swimlaneDispatcher.queues[topic][swimlane]).to.be.instanceOf(ObservableQueue);
    });
  });
});

function pushMessageToResults(message) {
  resultsBySwimlane[message.partitionId].push(message.payload);
}

function generateMessages() {
  const promises = [];
  Object.keys(messagesBySwimlane)
    .forEach((swimlane) => {
      messagesBySwimlane[swimlane].forEach((payload) => {
        const fakeMessagePromise = helpers.fakeKafkaMessage({
          topic,
          eventAggregateType,
          eventType,
          partition: swimlane,
          payload
        });
        promises.push(fakeMessagePromise)
      });
    });
  return Promise.all(promises);
}

function sendMessages(messages) {
  return messages.reduce(sequenceKafkaSend, Promise.resolve());
}

function sequenceKafkaSend(promise, message) {
  return new Promise((resolve) => {
    resolve(promise.then(_ => kafkaProducer.send(topic, message, message.partition)));
  });
}