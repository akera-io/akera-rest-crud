var JSDOCatalog = require('./metadata.js');
var FilterParser = require('../filter-parser.js');
var rsvp = require('rsvp');

function JSDOHandler(akera) {

  var self = this;

  this.akera = akera;
  this.crudHandler = akera.getAkeraHandler().getDataAccess();
  this.akeraMetadata = akera.getAkeraHandler().getMetaData();
  this.metadata = new JSDOCatalog(this.akeraMetadata);

  this.init = function(config, router) {
    self.asDataset = config.jsdo && config.jsdo.asDataset === false ? false : true;
    router.get(config.route + 'jsdo/metadata', self.getCatalog);
    router.get(config.route + 'jsdo/metadata/:db', self.getCatalog);
    router.get(config.route + 'jsdo/metadata/:db/:table', self.getCatalog);
    router.get(config.route + 'jsdo/:db/:table*', self.doSelect);
    router.post(config.route + 'jsdo/:db/:table', self.doCreate);
    router.put(config.route + 'jsdo/:db/:table/count', self.doCount);
    router.put(config.route + 'jsdo/:db/:table', self.doUpdate);
    router['delete'](config.route + 'jsdo/:db/:table', self.doDelete);
  };

  this.getCatalog = function(req, res) {
    var tableName = req.params.table;
    var dbName = req.params.db;

    self.metadata.getCatalog(dbName, tableName, self.asDataset === true,
      req.broker).then(function(catalog) {
      res.status(200).json(catalog);
    }, function(err) {
      self.akera.error(err, res);
    });
  };

  this.doSelect = function(req, res) {
    var tableName = req.params.db + '.' + req.params.table;

    var filter = {};
    var where = FilterParser.convert(req.query.filter);
    var limit = null;
    var offset = null;

    try {
      var rollbaseFilter = JSON.parse(req.query.filter);

      limit = getNumber(rollbaseFilter.top);
      offset = getNumber(rollbaseFilter.skip);

      if (rollbaseFilter.orderBy) {
        filter.sort = rollbaseFilter.orderBy.split(',').map(function(sort) {
          var by = sort.trim().split(' ');

          if (by.length === 1)
            return by;

          var sortBy = {};
          sortBy[by[0]] = by[by.length - 1] === 'desc';

          return sortBy;
        });
      }
    } catch (err) {}

    if (where) {
      filter.where = where;
    }

    limit = limit || getNumber(req.query.top);
    offset = offset || getNumber(req.query.skip);

    if (limit) {
      filter.limit = parseInt(limit);
    }

    if (offset) {
      filter.offset = parseInt(offset);
    }

    if (req.query.sort) {
      if (typeof (req.query.sort) === 'string' && req.query.sort !== '') {
        try {
          req.query.sort = JSON.parse(req.query.sort);
        } catch (e) {
          filter.sort = {
            field : req.query.sort
          };
        }
      }
      if (req.query.sort instanceof Array) {
        if (req.query.sort.length > 0) {
          filter.sort = req.query.sort.map(function(sortCriteria) {
            var sortCondition = {};
            sortCondition[sortCriteria.field] = sortCriteria.dir !== 'asc';
            return sortCondition;
          });
        }
      }
    }

    if (!self.asDataset) {
      _getPkFromQueryString(req).then(function(pkMap) {
        filter.pk = pkMap;
        return self.crudHandler.read(req.broker, tableName, filter);
      }).then(function(rows) {
        _sendReadResponse(rows, req, res);
      })['catch'](function(err) {
        self.akera.error(err, res);
      });
    } else {
      self.crudHandler.read(req.broker, tableName, filter).then(function(rows) {
        _sendReadResponse(rows, req, res);
      })['catch'](function(err) {
        self.akera.error(err, res);
      });
    }
  };

  this.doCreate = function(req, res) {
    if (req.body) {
      var tableName = req.params.db + '.' + req.params.table;

      delete req.body._id;
      var newObject;
      try {
        newObject = self.asDataset === true ? _getDatasetRow(req, 'created')
          : req.body;
      } catch (e) {
        return self.akera.error(e, res);
      }
      self.crudHandler.create(req.broker, tableName, newObject).then(
        function(row) {
          _sendReadResponse(row, req, res);
        }, function(err) {
          self.akera.error(err, res);
        });
    } else {
      self.akera.error(new Error('No data provided'), res);
    }
  };

  this.doUpdate = function(req, res) {
    if (req.body) {
      var tableName = req.params.db + '.' + req.params.table;

      delete req.body._id;
      var pkFn = self.asDataset ? _getPkFromBeforeImage : _getPkFromQueryString;
      pkFn(req).then(
        function(pkMap) {
          return self.crudHandler.update(req.broker, tableName, pkMap,
            self.asDataset ? _getDatasetRow(req, 'modified') : req.body);
        }).then(function(rows) {
        _sendReadResponse(rows, req, res);
      })['catch'](function(err) {
        self.akera.error(err, res);
      });
    }
  };

  this.doDelete = function(req, res) {
    var pkFn = self.asDataset ? _getPkFromBeforeImage : _getPkFromQueryString;
    var tableName = req.params.db + '.' + req.params.table;

    pkFn(req).then(function(pkMap) {
      return self.crudHandler.destroy(req.broker, tableName, pkMap);
    }).then(function(result) {
      res.status(200).json(result);
    })['catch'](function(err) {
      self.akera.error(err, res);
    });
  };

  this.doCount = function(req, res) {
    var tableName = req.params.db + '.' + req.params.table;
    var where = FilterParser.convert(req.query.filter);
    var filter = {
      count : true
    };

    if (where) {
      filter.where = where;
    }

    self.crudHandler.read(req.broker, tableName, filter).then(function(count) {
      res.status(200).json({
        response : {
          numRecs : count
        }
      });
    }, function(err) {
      self.akera.error(err, res);
    });
  };

  function getNumber(value) {
    if (typeof value === 'number')
      return value;

    if (typeof value === 'string') {
      value = value.trim();

      if (value.length > 0 && !isNaN(value))
        return parseFloat(value);
    }
  }

  function _getDatasetRow(req, state) {
    var tableName = req.params.table;
    var ds = req.body['ds' + tableName];
    var tts = ds && ds[tableName];

    if (tts instanceof Array) {
      var rows = tts.filter(function(row) {
        return row['prods:rowState'] === undefined || row['prods:rowState'] === state;
      });

      if (rows.length !== 1) {
        if (rows.length > 1)
          throw new Error('More than one record sent in request.');
        else
          throw new Error('No record found in request.');
      }

      var data = rows[0];

      Object.keys(data).forEach(function(field) {
        if (field.startsWith('prods:'))
          delete data[field];
      });

      return data;
    }

    throw new Error('Invalid table name or invalid request body specified.');

  }

  function _getPkFromQueryString(req) {
    return new rsvp.Promise(function(resolve, reject) {
      var pkString = req.params[0];
      if (!pkString)
        return resolve();
      if (pkString.charAt(pkString.length - 1) === '/') {
        pkString = pkString.substring(0, pkString.length - 1);
      }
      if (pkString.charAt(0) === '/') {
        pkString = pkString.substring(1, pkString.length);
      }
      var pk = pkString.split('/');

      _getPrimaryKey(req.broker, req.params.db, req.params.table).then(
        function(primaryKey) {
          if (pk.length !== primaryKey.length) {
            reject(new Error('Invalid primary key values'));
          }
          var pkMap = {};
          for ( var i in pk) {
            pkMap[primaryKey[i]] = pk[i];
          }
          resolve(pkMap);
        }, reject);
    });
  }

  function _getPrimaryKey(broker, db, table) {
    return new rsvp.Promise(function(resolve, reject) {
      self.akeraMetadata.getTable(broker, db, table).then(function(meta) {
        resolve(meta.pk);
      }, reject);
    });
  }

  function _getPkFromBeforeImage(req) {
    return new rsvp.Promise(function(resolve, reject) {
      var tableName = req.params.table;
      var ds = req.body['ds' + tableName];
      var before = null;

      if (ds) {
        if (ds['prods:before'])
          before = ds['prods:before'][tableName][0];
        else
          before = ds[tableName][0];
      } else {
        before = req.body[0];
      }

      _getPrimaryKey(req.broker, req.params.db, tableName).then(
        function(primaryKey) {
          var pkMap = {};
          for ( var i in primaryKey) {
            pkMap[primaryKey[i]] = before[primaryKey[i]];
          }
          resolve(pkMap);
        }, reject);
    });
  }

  function _sendReadResponse(rows, req, res) {
    var tableName = req.params.table;
    var result = {};

    if (self.asDataset === true) {
      var ds = {};

      ds[tableName] = rows instanceof Array ? rows : [ rows ];
      result['ds' + tableName] = ds;
    } else {
      result[tableName] = rows instanceof Array ? rows : [ rows ];
    }

    res.status(200).json(result);
  }

}

module.exports = JSDOHandler;
