var AkeraMetaData = require('./metadata.js');
var AkeraCrud = require('./crud.js');

function AkeraHandler() {
  
}

AkeraHandler.prototype.getDataAccess = function () {
  if (!this.dataAccess)
    this.dataAccess = new AkeraCrud();
  
  return this.dataAccess;
}

AkeraHandler.prototype.getMetaData = function () {
  if (!this.metadata)
    this.metadata = new AkeraMetaData();
  
  return this.metadata;
}

AkeraHandler.prototype.init = function (config, router) {
  
}

module.exports = AkeraHandler;
