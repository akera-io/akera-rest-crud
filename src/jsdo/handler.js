var JSDOCatalog = require('./metadata.js');
var metadata = new JSDOCatalog();

function JSDOHandler() {

  this.getCatalog = function(req, res) {
    var tableName = req.params.table;
    var dbName = req.params.db;

    metadata.getCatalog(dbName, tableName, req.broker, res);
  };
  
}

module.exports = JSDOHandler;
