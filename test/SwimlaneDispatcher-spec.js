const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
const MessageConsumer = require('../lib/kafka/MessageConsumer');
const SwimlaneDispatcher = require('../lib/kafka/SwimlaneDispatcher');
const KafkaProducer = require('../lib/kafka/KafkaProducer');

chai.use(chaiAsPromised);

const messageConsumer = new MessageConsumer();
const kafkaProducer = new KafkaProducer();

const timeout = 20000;
const topic = 'test-topic';
const eventAggregateType = 'Account';
const eventType = 'charge';
const expectedProcessedMessagesNum = 4;

before(async () => {
  await kafkaProducer.connect();
});

after(async () => {
  await Promise.all([
    messageConsumer.disconnect(),
    kafkaProducer.disconnect()
  ]);
});

describe('SwimlaneDispatcher', function () {
  this.timeout(timeout);

  it('should dispatch a message', async () => {
    const subscriberId = 'test-sb-id';

    return new Promise(async (resolve, reject) => {
      let processedNum = 0;
      const messageHandlers = {
        [topic]: (message) => {
          console.log('Processing queue message:', message);
          return Promise.resolve()
            .then(() => {
              console.log('--------------------------------');
              console.log('DISPATCHED');
              processedNum++;
              if (processedNum >= expectedProcessedMessagesNum) {
                resolve();
              }
            });
        }
      };

      const swimlaneDispatcher = new SwimlaneDispatcher({ messageHandlers });
      const messageHandler = (message) => swimlaneDispatcher.dispatch(message);

      try {
        await messageConsumer.subscribe({ subscriberId, topics: [ topic ], messageHandler });

        const messages = await Promise.all(Array.from(new Array(expectedProcessedMessagesNum))
          .map(() => helpers.fakeKafkaMessage(topic, eventAggregateType, eventType,
            JSON.stringify({ message: 'Test kafka subscription' })))
        );
        messages.forEach(message => kafkaProducer.send(topic, message));
      } catch (err) {
        reject(err);
      }
    });
  });
});
