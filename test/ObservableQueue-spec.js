const chai = require('chai');
const randomInt = require('random-int');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
const { ObservableQueue } = require('../');

const topic = 'topic1';
const executor = {};
const swimlane = 1;

describe('ObservableQueue', function () {
  this.timeout(10000);

  it('should throw an exception if no message handler for the topic', (done) => {
    const messageHandlers = {};
    try {
      new ObservableQueue({ messageHandlers, topic, executor, swimlane});
      done(new Error('Should throw exception'));
    } catch (e) {
      expect(e.message).eq(`Message handler not provided for topic "${topic}"`);
      done();
    }
  });

  it('call message handler for each message and verify total result', async () => {
    let result = 0;
    const messageHandlers = {
      [topic]: (message) => {
        result += message.val;
        return Promise.resolve();
      }
    };

    const queue = new ObservableQueue({ messageHandlers, topic, executor, swimlane});

    const values = [ 5, 3, 2, 10, 6 ];
    const expectedResult = values.reduce((acc, v) => (acc + v));
    const messages = values.map(val => ({ val, topic }));

    await Promise.all(messages.map((message) => {
      return new Promise((resolve, reject) => {
        queue.queueMessage({ message, resolve, reject });
      });
    }));

    expect(result).eq(expectedResult);
  });

  it('verify messages processed sequentially', async () => {
    const values = [ 100, 5, 20, 10, 6 ];
    const results = [];

    const messageHandlers = {
      [topic]: async (message) => {
          const timeout = randomInt(100, 1000);
          console.log('sleep ' + timeout);
          await helpers.sleep(timeout);
          console.log('processed message:', message);
          results.push(message.val);
          Promise.resolve();
      }
    };

    const queue = new ObservableQueue({ messageHandlers, topic, executor, swimlane});
    const messages = values.map(val => ({ val, topic }));

    await Promise.all(messages.map((message) => {
      return new Promise((resolve, reject) => {
        queue.queueMessage({ message, resolve, reject });
      });
    }));

    expect(results.length).eq(values.length);
    results.forEach((v, index) => expect(v).eq(values[index]));
  });
});