var AkeraMetaData = require('./metadata.js');
var AkeraCrud = require('./crud.js');

function AkeraHandler(akeraRest) {
  var self = this;

  this.akeraRest = akeraRest;

  this.getDataAccess = function() {
    if (!this.dataAccess)
      this.dataAccess = new AkeraCrud();

    return this.dataAccess;
  };

  this.getMetaData = function() {
    if (!this.metadata)
      this.metadata = new AkeraMetaData();

    return this.metadata;
  };

  this.init = function(config, router) {

    router.get(config.route + 'metadata', self.getDatabases);
    router.get(config.route + 'metadata/:db', self.getTables);
    router.get(config.route + 'metadata/:db/:table', self.getFields);
    router
      .get(config.route + 'metadata/:db/:table/index(es)?', self.getIndexes);

    router.get(config.route + ':db/:table', self.doSelect);
    router.get(config.route + ':db/:table/count', self.doCount);
    router.get(config.route + ':db/:table/*', self.doSelectRecord);
    router.post(config.route + ':db/:table', self.doCreate);
    router.put(config.route + ':db/:table/rowid/:id', self.doUpdateByRowid);
    router.put(config.route + ':db/:table/*', self.doUpdate);
    router['delete'](config.route + ':db/:table/rowid/:id',
      self.doDeleteByRowid);
    router['delete'](config.route + ':db/:table/*', self.doDelete);

  };

  this.getDatabases = function(req, res) {
    self.getMetaData().getDatabases(req.broker).then(function(info) {
      res.status(200).json(Object.keys(info));
    }, function(err) {
      self.akeraRest.error(err, res);
    });
  };

  this.getTables = function(req, res) {
    var db = isNaN(req.params.db) ? req.params.db : parseInt(req.params.db);

    self.getMetaData().getTables(req.broker, db).then(function(info) {
      res.status(200).json(Object.keys(info));
    }, function(err) {
      self.akeraRest.error(err, res);
    });
  };

  this.getFields = function(req, res) {
    var db = isNaN(req.params.db) ? req.params.db : parseInt(req.params.db);
    var table = isNaN(req.params.table) ? req.params.table
      : parseInt(req.params.table);

    self.getMetaData().getTable(req.broker, db, table).then(function(info) {
      res.status(200).json(info.fields);
    }, function(err) {
      self.akeraRest.error(err, res);
    });
  };

  this.getIndexes = function(req, res) {
    var db = isNaN(req.params.db) ? req.params.db : parseInt(req.params.db);
    var table = isNaN(req.params.table) ? req.params.table
      : parseInt(req.params.table);

    self.getMetaData().getTable(req.broker, db, table).then(function(info) {
      res.status(200).json(info.indexes);
    }, function(err) {
      self.akeraRest.error(err, res);
    });
  };

  this.doSelect = function(req, res) {
    var table = req.params.db + '.' + req.params.table;
    var filter = (req.query && req.query.filter) || {};

    self.getDataAccess().read(req.broker, table, filter).then(function(info) {
      res.status(200).json(info);
    }, function(err) {
      self.akeraRest.error(err, res);
    });
  };

  this.doCount = function(req, res) {
    var table = req.params.db + '.' + req.params.table;
    var filter = (req.query && req.query.filter) || {};

    delete filter.sort;
    filter.count = true;

    self.getDataAccess().read(req.broker, table, filter).then(function(info) {
      res.status(200).json({
        count : info
      });
    }, function(err) {
      self.akeraRest.error(err, res);
    });
  };

  this.doSelectRecord = function(req, res) {
    var tableName = req.params.db + '.' + req.params.table;
    var filter = (req.query && req.query.filter) || {};
    var pkValues = req.params[0] ? req.params[0].toString().split('/') : [];

    self.getMetaData().getTable(req.broker, req.params.db, req.params.table)
      .then(function(table) {
        filter.pk = self.getPkWhere(table, pkValues);
        return self.getDataAccess().read(req.broker, tableName, filter);
      }).then(function(info) {
        res.status(200).json(info);
      })['catch'](function(err) {
      self.akeraRest.error(err, res);
    });
  };

  this.doCreate = function(req, res) {
    var table = req.params.db + '.' + req.params.table;

    self.getDataAccess().create(req.broker, table, req.body).then(
      function(info) {
        res.status(200).json(info);
      }, function(err) {
        self.akeraRest.error(err, res);
      });
  };

  this.doUpdate = function(req, res) {
    var tableName = req.params.db + '.' + req.params.table;
    var pkValues = req.params[0] ? req.params[0].toString().split('/') : [];

    self.getMetaData().getTable(req.broker, req.params.db, req.params.table)
      .then(
        function(table) {
          return self.getDataAccess().update(req.broker, tableName,
            self.getPkWhere(table, pkValues), req.body);
        }).then(function(info) {
        res.status(200).json(info);
      })['catch'](function(err) {
      self.akeraRest.error(err, res);
    });
  };

  this.doUpdateByRowid = function(req, res) {
    var table = req.params.db + '.' + req.params.table;

    return self.getDataAccess().update(req.broker, table, req.params.id,
      req.body).then(function(info) {
      res.status(200).json(info);
    }, function(err) {
      self.akeraRest.error(err, res);
    });
  };

  this.doDelete = function(req, res) {
    var tableName = req.params.db + '.' + req.params.table;
    var pkValues = req.params[0] ? req.params[0].toString().split('/') : [];

    self.getMetaData().getTable(req.broker, req.params.db, req.params.table)
      .then(
        function(table) {
          return self.getDataAccess().destroy(req.broker, tableName,
            self.getPkWhere(table, pkValues));
        }).then(function(info) {
        res.status(200).json({
          updated : info
        });
      })['catch'](function(err) {
      self.akeraRest.error(err, res);
    });
  };

  this.doDeleteByRowid = function(req, res) {
    var table = req.params.db + '.' + req.params.table;

    return self.getDataAccess().destroy(req.broker, table, req.params.id).then(
      function(info) {
        res.status(200).json({
          updated : info
        });
      }, function(err) {
        self.akeraRest.error(err, res);
      });
  };

  this.getPkWhere = function(table, pkValues) {
    if (!table.pk || table.pk.length === 0)
      throw new Error('Table does not have a primary key.');

    if (table.pk.length !== pkValues.length)
      throw new Error('Invalid primary key values.');

    var pkFilter = {};

    try {
      for ( var key in table.pk) {
        pkFilter[table.pk[key]] = pkValues[key];
      }
    } catch (err) {
      throw new Error('Invalid primary key data.');
    }

    return pkFilter;
  };

}

module.exports = AkeraHandler;
