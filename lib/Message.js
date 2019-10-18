
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

  hasHeader(name) {
    return typeof (headers[name]) !== 'undefined';
  }

  getId() {
    return this.getRequiredHeader(Message.ID);
  }

  getHeaders() {
    return headers;
  }

  setPayload(payload) {
    this.payload = payload;
  }

  setHeaders(headers) {
    this.headers = headers;
  }

  setHeader(name, value) {
    headers[name] =value;
  }

  removeHeader(key) {
    delete headers[key];
  }

  static get ID()  {
    return 'ID';
  }

  static get PARTITION_ID() {
    return 'PARTITION_ID';
  }

  static DESTINATION() {
    return 'DESTINATION';
  }

  static get DATE() {
    return 'DATE';
  }
}

module.exports = Message;