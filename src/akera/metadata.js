var akera = require('akera-api');
var async = require('async');
var rsvp = require('rsvp');

function AkeraMetaData(broker) {
  this.broker = broker;
}

AkeraMetaData.prototype.getDatabases = function(fullLoad) {
  
}

AkeraMetaData.prototype.getTables = function(db, fullLoad) {
  
}

AkeraMetaData.prototype.getTable = function(db, table) {
  
}
  
module.exports = AkeraMetaData;
