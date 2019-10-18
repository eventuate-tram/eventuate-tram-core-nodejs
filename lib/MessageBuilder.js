class MessageBuilder {
  constructor(body) {
    this.body = body;
    this.headers = [];
  }

  withHeader(name, value) {
    this.headers.push({ [name]: value });
  }
}

module.exports = MessageBuilder;