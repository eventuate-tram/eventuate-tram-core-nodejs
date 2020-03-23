const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
const { MessageConsumer, MessageProducer, DomainEventDispatcher, DomainEventPublisher } = require('../');

chai.use(chaiAsPromised);

const messageProducer = new MessageProducer();
const domainEventPublisher = new DomainEventPublisher({ messageProducer });
const messageConsumer = new MessageConsumer();

const aggregateType = 'Account';
const aggregateId = 'Fake_aggregate_id';

const timeout = 20000;

describe('DomainEventDispatcher', function () {
  this.timeout(timeout);

  after(async () => {
    return messageConsumer.unsubscribe();
  });

  it('should dispatch an event', async () => {
    return new Promise(async (resolve) => {

      const eventType = 'CreditApproved';
      const expectedEvent = { amount: 10, _type: eventType };
      const eventDispatcherId = 'test-domain-event-dispatcher-id';

      const domainEventHandlers = {
        [aggregateType]: {
          [eventType]: (event) => {
            helpers.expectDomainEvent(event, expectedEvent);
            resolve();
          }
        }
      };

      const domainEventDispatcher = new DomainEventDispatcher({ eventDispatcherId,
        domainEventHandlers,
        messageConsumer
      });
      await domainEventDispatcher.initialize();
      await domainEventPublisher.publish(aggregateType, aggregateId, [ expectedEvent ]);
    });
  });

  it( 'should dispatch an event with custom domain event mapping', async () => {
    return new Promise(async (resolve) => {
      const eventType = 'CreditApproved';
      const expectedEvent = { amount: 10, _type: eventType };
      const eventDispatcherId = 'test-domain-event-dispatcher-event-mapping-id';

      const domainEventHandlers = {
        [aggregateType]: {
          ['CustomerCreditApproved']: (event) => {
            helpers.expectDomainEvent(event, expectedEvent);
            resolve();
          }
        }
      };

      class CustomerDomainEventNameMapping {
        constructor() {
          this.mappings = {
            [aggregateType]: new Map([[eventType, 'CustomerCreditApproved']])
          };
        }
        externalEventTypeToEvent(aggregateType, eventTypeHeader) {
          if (this.mappings[aggregateType]) {
            return this.mappings[aggregateType].get(eventTypeHeader);
          }
          throw new Error('Unknown aggregate type');
        }
      }

      const domainEventDispatcher = new DomainEventDispatcher({ eventDispatcherId,
        domainEventHandlers,
        messageConsumer,
        domainEventNameMapping: new CustomerDomainEventNameMapping()
      });
      await domainEventDispatcher.initialize();
      await domainEventPublisher.publish(aggregateType, aggregateId, [ expectedEvent ]);
    });
  });
});
