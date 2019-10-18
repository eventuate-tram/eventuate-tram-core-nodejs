const Message = require('./Message');

class MessageBuilder {
  constructor(body) {
    this.body = body;
    this.headers = {};
  }

  build() {
    return new Message({ payload: this.body, headers: this.headers });
  }

  static withHeader(name, value) {
    this.headers[name] = value;
    return this;
  }

  static withPayload(payload) {
    return new MessageBuilder(payload);
  }

  withExtraHeaders(prefix, headers) {

    Object.keys(headers).forEach((key) => {
      this.headers[prefix + key] = headers[key];
    });
    return this;
  }
}

module.exports = MessageBuilder;