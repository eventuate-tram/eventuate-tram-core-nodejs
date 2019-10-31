const Rx = require('rx');
const { getLogger } = require('./utils');

class ObservableQueue {
  constructor({ topic, swimlane, messageHandlers, executor }) {

    if (!messageHandlers[topic]) {
      throw new Error(`Message handler not provided for topic "${topic}"`);
    }

    this.topic = topic;
    this.swimlane = swimlane;
    this.messageHandlers = messageHandlers;
    this.executor = executor;

    this.logger = getLogger({ title: `Queue-${this.topic}-${this.swimlane}` });

    const observable = Rx.Observable.create(this.observableCreateFn.bind(this));

    observable
      .map(this.createObservableHandler())
      .merge(1)
      .subscribe(
        ({ resolve }) => {
          resolve();
        },
        ({ err, reject}) => {
          this.logger.error('Message handler error:', err);
          reject(err);
        },
        () => this.logger.debug('Disconnected!')
      );
  }

  observableCreateFn(observer) {
    this.observer = observer;
  }

  queueMessage({ message, resolve, reject }) {
    this.observer.onNext({ message, resolve, reject });
  }

  createObservableHandler() {
    return ({ message, resolve, reject }) => Rx.Observable.create(observer => {
      const { topic } = message;
      const messageHandler = this.messageHandlers[topic];

      if (!messageHandler) {
        return reject(new Error(`No message handler for topic: ${topic}`));
      }

      try {
        messageHandler.call(this.executor, message)
          .then((result) => {
              observer.onNext({ message, resolve });
              observer.onCompleted();
            })
          .catch(err => {
            observer.onError({ err, reject });
          });
      } catch (err) {
        observer.onError({ err, reject });
      }
    });
  }
}

module.exports = ObservableQueue;
