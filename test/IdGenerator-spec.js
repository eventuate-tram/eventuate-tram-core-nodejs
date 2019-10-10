const bPromise = require('bluebird');
const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);

const { expectEventId, onlyUnique } = require('./lib/helpers');

const IdGenerator = require('../lib/IdGenerator');

const idGenerator = new IdGenerator();
const timeout = 15000;

describe('IdGenerator test', function () {
  this.timeout(timeout);

  it('should generate an ID', async () => {
    const id = await idGenerator.genIdInternal();
    expectEventId(id);
  });

  it('should generate MAX_COUNTER ID\'s', async () => {
    console.log('MAX_COUNTER: ', IdGenerator.maxCounter);
    const ids = await genIds(3000);
    console.log(`Generated ${ids.length} ID's`);
    const uniqueIds = ids.filter(onlyUnique);
    expect(uniqueIds.length).eq(ids.length);
    expect(uniqueIds).to.deep.equal(ids);
  });
});

function genIds(number) {
  return bPromise.mapSeries(new Array(number), () => {
    return idGenerator.genIdInternal();
  });
}
