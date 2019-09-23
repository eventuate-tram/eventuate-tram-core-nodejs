const { addZeroes, promisedDelay, getLogger } = require('./utils');
const getmac = require('getmac');

const logger = getLogger({ title: 'IdGenerator', logLevel: 'debug' });

const MAX_COUNTER = 1 << 16;

class IdGenerator {
  constructor() {
    this.currentPeriod = this.timeNow();
    this.counter = 0;
  }

  toLong(macAddressStr) {
    let bytes = [];

    for (let i = 0; i < macAddressStr.length; ++i) {
      const charCode = macAddressStr.charCodeAt(i);
      bytes.push(charCode);
    }

    let result = 0;

    bytes.forEach(b => {
      result = (result << 8) + b;
    });

    return result;
  }

  initializeMacAddress() {
    return new Promise((resolve, reject) => {
      if (this.macAddress) {
        return resolve();
      }

      getmac.getMac((err, macAddress) => {
        if (err) {
          return reject(err);
        }

        logger.info('Host mac address: ', { macAddress });
        this.macAddress = this.toLong(macAddress.replace(/:/g, ''));
        logger.info(`this.macAddress: ${this.macAddress}`);
        resolve();
      })
    });
  }

  async genIdInternal() {
    await this.initializeMacAddress();
    const now = this.timeNow();
    logger.debug('this.currentPeriod:', this.currentPeriod);

    if (this.currentPeriod !== now) { // different millisecond so reset the count
      this.currentPeriod = now;
      this.counter = 0;
    } else if (this.counter === MAX_COUNTER) { // same millisecond but counter at MAX so wait for new millisecond
      logger.debug('this.counter == MAX_COUNTER');
      const oldPeriod = this.currentPeriod;

      await promisedDelay(1);

      if ((this.timeNow() !== now) && (oldPeriod === this.currentPeriod)) {  /// we are in the future
        /// No one else has done this yet- another call might have changed things.
        this.currentPeriod = this.timeNow();
        this.counter = 0;
      }
      return this.genIdInternal();
    }

    const id = this.makeId();
    this.counter = this.counter + 1;

    return id;
  }

  timeNow() {
    return new Date().getTime();
  }

  makeId() {
    const currentPeriodHex = this.currentPeriod.toString(16);

    const part1 = addZeroes({
      src: currentPeriodHex,
      position: 'begin'
    });

    const part2 = addZeroes({
      src: (this.macAddress << 16) + this.counter,
      position: 'end'
    });

    return `${part1}-${part2}`;
  }

  static get maxCounter() {
    return MAX_COUNTER;
  }
}

module.exports = IdGenerator;