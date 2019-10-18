
class Message {
  constructor({ payload, headers }) {
    this.payload = payload;
    this.headers = headers;
  }

  toString() {
    return JSON.stringify({ payload: this.payload, headers: this.headers });
  }

  getPayload() {
    return this.payload;
  }

  getHeader(name) {
    return this.headers[name];
  }

  getRequiredHeader(name) {
    const header = this.headers[name];
    if (!header) {
      throw `No such header "${name}" in this message "${this}`;
    }

    return header;
  }

  public hasHeader(name) {
    return typeof (headers[name]) !== 'undefined';
  }

  public getId() {
    return getRequiredHeader(Message.ID);
  }
}

module.exports = Message;