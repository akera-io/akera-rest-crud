var JSDOCatalog = require('./metadata.js');
var rsvp = require('rsvp');

function JSDOHandler(akera) {

  var self = this;

  this.akera = akera;
  this.crudHandler = akera.getAkeraHandler().getDataAccess();
  this.akeraMetadata = akera.getAkeraHandler().getMetaData();
  this.metadata = new JSDOCatalog(this.akeraMetadata);

  this.init = function(config, router) {
    self.asDataset = config.jsdo && config.jsdo.asDataset === true;
    router.get(config.route + 'jsdo/metadata', self.getCatalog);
    router.get(config.route + 'jsdo/metadata/:db', self.getCatalog);
    router.get(config.route + 'jsdo/metadata/:db/:table', self.getCatalog);
    router.use(config.route + 'jsdo/:db/:table/count', self.doCount);
    router.get(config.route + 'jsdo/:db/:table*', self.doSelect);
    router.post(config.route + 'jsdo/:db/:table', self.doCreate);
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
    if (req.query.filter && req.query.filter !== '') {
      filter = self.filter.fromKendo(req.query.filter);
    }

    if (req.query.top && req.query.top !== '' && !isNaN(req.query.top)) {
      filter.limit = parseInt(req.query.top);
    }
    if (req.query.skip && req.query.skip !== '' && !isNaN(req.query.skip)) {
      filter.offset = parseInt(req.query.skip);
    }

    if (req.query.sort) {
      if (typeof (req.query.sort) === 'string' && req.query.sort !== '') {
        try {
          req.query.sort = JSON.parse(req.query.sort);
        } catch (e) {
          filter.by = {
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
        newObject = self.asDataset === true ? _getDataFromDataset(req)
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
            self.asDataset ? _getUpdateDataFromDsUpdate(req) : req.body);
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
    var filter = req.query.filter && req.query.filter !== '' ? self.filter
      .fromKendo(req.query.filter) : {};
    filter.count = true;
    self.crudHandler.read(req.broker, tableName, filter).then(function(count) {
      res.status(200).json({
        count : count
      });
    }, function(err) {
      self.akera.error(err, res);
    });
  };

  this.filter = {
    fromKendo : function(kendoFilter) {
      return {
        where : _convertKendoFilter(kendoFilter)
      };
    }
  };

  function _getUpdateDataFromDsUpdate(req) {
    var update = req.body['ds' + req.params.table]['tt' + req.params.table][0];
    delete update['prods:clientId'];
    delete update['prods:id'];
    delete update['prods:rowState'];
    return update;
  }

  function _getDataFromDataset(req) {
    var tts = req.body[_ttName(req.params.table)];
    if (!tts)
      throw new Error(
        'Invalid table name or invalid request body specified. Request body must have property '
          + _ttName(req.params.table));
    if (tts instanceof Array) {
      tts = tts[0];
    }
    delete tts._id;
    return tts;
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
      var before = req.body['ds' + req.params.table]['prods:before']['tt'
        + req.params.table][0];
      _getPrimaryKey(req.broker, req.params.db, req.params.table).then(
        function(primaryKey) {
          var pkMap = {};
          for ( var i in primaryKey) {
            pkMap[primaryKey[i]] = before[primaryKey[i]];
          }
          resolve(pkMap);
        }, reject);
    });
  }

  function _dsName(tableName) {
    return 'ds' + tableName;
  }

  function _ttName(tableName) {
    return 'tt' + tableName;
  }

  function _sendReadResponse(rows, req, res) {
    var table = req.params.table;
    if (self.asDataset === true) {
      var ds = {};
      ds[_dsName(table)] = {};
      ds[_dsName(table)][_ttName(table)] = rows instanceof Array ? rows
        : [ rows ];
      return res.status(200).json(ds);
    }

    var ret = {};
    ret[table] = rows instanceof Array ? rows : [ rows ];
    res.status(200).json(ret);
  }

  function _getClauseFromKendo(flt) {

    if (!flt || !flt.operator)
      return null;

    var clause = {};
    switch (flt.operator) {
      case 'eq':
        clause[flt.field] = {
          eq : flt.value
        };
        break;
      case 'neq':
        clause[flt.field] = {
          ne : flt.value
        };
        break;
      case 'gte':
        clause[flt.field] = {
          ge : flt.value
        };
        break;
      case 'lte':
        clause[flt.field] = {
          le : flt.value
        };
        break;
      case 'lt':
        clause[flt.field] = {
          lt : flt.value
        };
        break;
      case 'gt':
        clause[flt.field] = {
          gt : flt.value
        };
        break;
      case 'contains':
        clause[flt.field] = {
          matches : '*' + flt.value + '*'
        };
        break;
      case 'doesnotcontain': {
        clause[flt.field] = {
          not : {
            matches : '*' + flt.value + '*'
          }
        };
        break;
      }
      case 'startswith':
        clause[flt.field] = {
          matches : flt.value + '*'
        };
        break;
      case 'endswith':
        clause[flt.field] = {
          matches : '*' + flt.value
        };
        break;
      default:
        throw new TypeError('Filter operator ' + flt.operator
          + ' is not supported.');
    }
    return clause;
  }

  function _convertKendoFilter(filter) {
    var restFilter = {};
    if (typeof (filter) === 'string') {
      filter = JSON.parse(filter);
    }
    if (filter.filters) {
      if (filter.filters.length === 1) {
        restFilter = _convertKendoFilter(filter.filters[0]);
        return restFilter;
      }
      restFilter[filter.logic] = [];

      filter.filters.forEach(function(flt) {
        restFilter[filter.logic].push(_convertKendoFilter(flt));
      });

    } else {
      return _getClauseFromKendo(filter);
    }

    return restFilter;
  }

}

module.exports = JSDOHandler;
