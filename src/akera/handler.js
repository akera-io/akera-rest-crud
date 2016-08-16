var AkeraMetaData = require('./metadata.js');
var AkeraCrud = require('./crud.js');

function AkeraHandler(akeraRest) {
  this.akeraRest = akeraRest;
}

AkeraHandler.prototype.getDataAccess = function() {
  if (!this.dataAccess)
    this.dataAccess = new AkeraCrud();

  return this.dataAccess;
};

AkeraHandler.prototype.getMetaData = function() {
  if (!this.metadata)
    this.metadata = new AkeraMetaData();

  return this.metadata;
};

AkeraHandler.prototype.init = function(config, router) {

  var self = this;

  router.get(config.route + 'metadata', self.getDatabases);
  router.get(config.route + 'metadata/:db', self.getTables);
  router.get(config.route + 'metadata/:db/:table', self.getFields);
  router.get(config.route + 'metadata/:db/:table/index(es)?', self.getIndexes);

  router.get(config.route + ':db/:table', self.doSelect);
  router.get(config.route + ':db/:table/count', self.doCount);
  router.get(config.route + ':db/:table/*', self.doSelectRecord);
  router.post(config.route + ':db/:table', self.doCreate);
  router.put(config.route + ':db/:table/rowid/:id', self.doUpdateByRowid);
  router.put(config.route + ':db/:table/*', self.doUpdate);
  router['delete'](config.route + ':db/:table/rowid/:id', self.doDeleteByRowid);
  router['delete'](config.route + ':db/:table/*', self.doDelete);

};

AkeraHandler.prototype.getDatabases = function(req, res) {
  var self = this;

  self.getMetaData().getDatabases(req.broker).then(function(info) {
    res.status(200).send(Object.keys(info));
  }, function(err) {
    self.akeraRest.error(err, res);
  });
};

AkeraHandler.prototype.getTables = function(req, res) {
  var self = this;

  var db = isNaN(req.params.db) ? req.params.db : parseInt(req.params.db);

  self.getMetaData().getTables(req.broker, db).then(function(info) {
    res.status(200).send(Object.keys(info));
  }, function(err) {
    self.akeraRest.error(err, res);
  });
};

AkeraHandler.prototype.getFields = function(req, res) {
  var self = this;

  var db = isNaN(req.params.db) ? req.params.db : parseInt(req.params.db);
  var table = isNaN(req.params.table) ? req.params.table
    : parseInt(req.params.table);

  self.getMetaData().getTable(req.broker, db, table).then(function(info) {
    res.status(200).send(info.fields);
  }, function(err) {
    self.akeraRest.error(err, res);
  });
};

AkeraHandler.prototype.getIndexes = function(req, res) {
  var self = this;

  var db = isNaN(req.params.db) ? req.params.db : parseInt(req.params.db);
  var table = isNaN(req.params.table) ? req.params.table
    : parseInt(req.params.table);

  self.getMetaData().getTable(req.broker, db, table).then(function(info) {
    res.status(200).send(info.indexes);
  }, function(err) {
    self.akeraRest.error(err, res);
  });
};

AkeraHandler.prototype.doSelect = function(req, res) {
  var self = this;

  var table = req.params.db + '.' + req.params.table;
  var filter = (req.query && req.query.filter) || {};

  self.getDataAccess().read(req.broker, table, filter).then(function(info) {
    res.status(200).send(info);
  }, function(err) {
    self.akeraRest.error(err, res);
  });
};

AkeraHandler.prototype.doCount = function(req, res) {
  var self = this;

  var table = req.params.db + '.' + req.params.table;
  var filter = (req.query && req.query.filter) || {};

  delete filter.sort;
  filter.count = true;

  self.getDataAccess().read(req.broker, table, filter).then(function(info) {
    res.status(200).send(info);
  }, function(err) {
    self.akeraRest.error(err, res);
  });
};

AkeraHandler.prototype.doSelectRecord = function(req, res) {
  var self = this;

  var tableName = req.params.db + '.' + req.params.table;
  var filter = (req.query && req.query.filter) || {};
  var pkValues = req.params[0] ? req.params[0].toString().split('/') : [];

  self.getMetaData().getTable(req.broker, req.params.db, req.params.table)
    .then(function(table) {
      var pkFilter = {};

      try {
        for ( var key in table.pk) {
          pkFilter[table.pk[key]] = pkValues[key];
        }
        filter.pk = pkFilter;

      } catch (err) {
        throw new Error('Invalid primary key data.');
      }

      return self.getDataAccess().read(req.broker, tableName, filter);
    }).then(function(info) {
      res.status(200).send(info);
    })['catch'](function(err) {
    self.akeraRest.error(err, res);
  });
};

AkeraHandler.prototype.doCreate = function(req, res) {
  var self = this;

  var table = req.params.db + '.' + req.params.table;

  self.getDataAccess().create(req.broker, table, req.body).then(function(info) {
    res.status(200).send(info);
  }, function(err) {
    self.akeraRest.error(err, res);
  });
};

AkeraHandler.prototype.doUpdate = function(req, res) {
  var self = this;

  var tableName = req.params.db + '.' + req.params.table;
  var pkValues = req.params[0] ? req.params[0].toString().split('/') : [];

  self.getMetaData().getTable(req.broker, req.params.db, req.params.table)
    .then(
      function(table) {
        var pkFilter = {};

        try {
          for ( var key in table.pk) {
            pkFilter[table.pk[key]] = pkValues[key];
          }
        } catch (err) {
          throw new Error('Invalid primary key data.');
        }

        return self.getDataAccess().update(req.broker, tableName, pkFilter,
          req.body);
      }).then(function(info) {
      res.status(200).send(info);
    })['catch'](function(err) {
    self.akeraRest.error(err, res);
  });
};

AkeraHandler.prototype.doUpdateByRowid = function(req, res) {
  var self = this;

  var table = req.params.db + '.' + req.params.table;

  return self.getDataAccess()
    .update(req.broker, table, req.params.id, req.body).then(function(info) {
      res.status(200).send(info);
    }, function(err) {
      self.akeraRest.error(err, res);
    });
};

AkeraHandler.prototype.doDelete = function(req, res) {
  var self = this;

  var tableName = req.params.db + '.' + req.params.table;
  var pkValues = req.params[0] ? req.params[0].toString().split('/') : [];

  self.getMetaData().getTable(req.broker, req.params.db, req.params.table)
    .then(function(table) {
      var pkFilter = {};

      try {
        for ( var key in table.pk) {
          pkFilter[table.pk[key]] = pkValues[key];
        }
      } catch (err) {
        throw new Error('Invalid primary key data.');
      }

      return self.getDataAccess().destroy(req.broker, tableName, pkFilter);
    }).then(function(info) {
      res.status(200).send(info);
    })['catch'](function(err) {
    self.akeraRest.error(err, res);
  });
};

AkeraHandler.prototype.doDeleteByRowid = function(req, res) {
  var self = this;

  var table = req.params.db + '.' + req.params.table;

  return self.getDataAccess().destroy(req.broker, table, req.params.id).then(
    function(info) {
      res.status(200).send(info);
    }, function(err) {
      self.akeraRest.error(err, res);
    });
};
module.exports = AkeraHandler;
