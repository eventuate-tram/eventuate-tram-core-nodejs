const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const randomInt = require('random-int');
const helpers = require('./lib/helpers');
const MessageConsumer = require('../lib/kafka/MessageConsumer');
const SwimlaneDispatcher = require('../lib/kafka/SwimlaneDispatcher');
const ObservableQueue = require('../lib/ObservableQueue');
const KafkaProducer = require('../lib/kafka/KafkaProducer');

chai.use(chaiAsPromised);

const messageConsumer = new MessageConsumer();
const kafkaProducer = new KafkaProducer();

const timeout = 25000;
const topic = 'test-topic-swimlane-dispatcher';
const eventAggregateType = 'Account';
const eventType = 'charge';
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
  1: [ 5, 10, 100 ]
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
        [topic]: (message) => {
          console.log('Processing queue message:', message);
          return Promise.resolve()
            .then(async () => {
              await randomSleep();
              pushMessageToResults(message);

              processedNum++;
              if (processedNum >= expectedProcessedMessagesNum) {
                console.log('resultsBySwimlane', resultsBySwimlane);
                console.log('messagesBySwimlane', messagesBySwimlane);
                expect(resultsBySwimlane).to.deep.eq(messagesBySwimlane);
                resolve();
              }
            });
        }
      };

      swimlaneDispatcher = new SwimlaneDispatcher({ messageHandlers });
      const messageHandler = (message) => swimlaneDispatcher.dispatch(message);

      try {
        await messageConsumer.subscribe({ subscriberId, topics: [ topic ], messageHandler });
        const messages = await generateMessages();
        sendMessages(messages);
      } catch (err) {
        reject(err);
      }
    });
  });

  it('should have queues for each swimlane', () => {
    expect(swimlaneDispatcher).to.be.ok;
    expect(swimlaneDispatcher).to.haveOwnProperty('queues');
    expect(swimlaneDispatcher.queues).to.haveOwnProperty(topic);
    console.log('swimlaneDispatcher.queues:', swimlaneDispatcher.queues);
    Object.keys(messagesBySwimlane).forEach((swimlane) => {
      expect(swimlaneDispatcher.queues[topic]).to.haveOwnProperty(swimlane);
      expect(swimlaneDispatcher.queues[topic][swimlane]).to.be.instanceOf(ObservableQueue);
    });
  });
});

function pushMessageToResults(message) {
  resultsBySwimlane[message.partitionId].push(message.payload);
}

async function randomSleep() {
  const timeout = randomInt(100, 1000);
  console.log('sleep ' + timeout);
  await helpers.sleep(timeout);
}

function generateMessages() {
  return Promise.all(
    Object.keys(messagesBySwimlane)
      .reduce((acc, swimlane) => {
        messagesBySwimlane[swimlane].forEach((m) => acc.push(helpers.fakeKafkaMessage({ topic, eventAggregateType, eventType, partition: swimlane, payload: m })));
        return acc;
      }, [])
  );
}

function sendMessages(messages) {
  messages.forEach(message => kafkaProducer.send(topic, message, message.partition));
}