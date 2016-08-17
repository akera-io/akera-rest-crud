
module.exports = function Request() {
  this.query = {};
  this.params = {};
  this.body = {};
  
  this.queryString = function(qry) {
    this.query = qry;
    return this;
  };
  
  this.reqParams = function(params) {
    this.params = params;
    return this;
  };
  
  this.reqBroker = function(broker) {
    this.broker = broker;
    return this;
  };
  
  this.reqBody = function(body) {
    this.body = body;
    return this;
  };
};