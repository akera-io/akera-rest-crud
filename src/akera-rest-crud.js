module.exports = AkeraRestCrud;

var akeraApi = require('akera-api');
var f = akeraApi.query.filter;
var akeraApp = null;

function AkeraRestCrud(akeraWebApp) {
  var self = this;
  var pkInfo = {};

  this.error = function(err, res) {
    if (err) {
      if (err instanceof Error) {
        err = err.message;
      }

      res.status(500).send({
        message : err
      });

      akeraApp.log('error', err);
    }
  };

  this.connect = function(broker, callback) {
    akeraApi.connect(broker).then(function(conn) {
      callback(null, conn);
    }, function(err) {
      callback(err);
    });
  };

  this.getDatabases = function(req, res) {
    self.connect(req.broker, function(err, conn) {
      if (err) {
        self.error(err, res);
      } else {
        conn.getMetaData().allDatabases().then(function(dbs) {
          conn.disconnect();
          res.status(200).send(dbs.map(function(db) {
            return db.getLname().toLowerCase();
          }));
        }, function(err) {
          conn.disconnect();
          self.error(err, res);
        });
      }
    });
  };

  this.getTables = function(req, res) {
    var db = isNaN(req.params.db) ? req.params.db : parseInt(req.params.db);

    self.connect(req.broker, function(err, conn) {
      if (err) {
        self.error(err, res);
      } else {
        conn.getMetaData().getDatabase(db).then(function(dbMeta) {
          return dbMeta.allTables();
        }, function() {
          conn.disconnect();
          res.status(404).send();
        }).then(function(tables) {
          conn.disconnect();
          res.status(200).send(tables.map(function(tbl) {
            return tbl.getName().toLowerCase();
          }));
        }, function(err) {
          conn.disconnect();
          self.error(err, res);
        });
      }
    });
  };

  this.getFields = function(req, res) {
    var db = isNaN(req.params.db) ? req.params.db : parseInt(req.params.db);
    var table = isNaN(req.params.table) ? req.params.table
        : parseInt(req.params.table);

    self.connect(req.broker, function(err, conn) {
      if (err) {
        self.error(err, res);
      } else {
        conn.getMetaData().getDatabase(db).then(function(dbMeta) {
          return dbMeta.getTable(table);
        }, function() {
          conn.disconnect();
          res.status(404).send();
        }).then(function(tbl) {
          return tbl.getAllFields();
        }, function() {
          conn.disconnect();
          res.status(404).send();
        }).then(function(fields) {
          conn.disconnect();
          res.status(200).send(fields);
        }, function(err) {
          conn.disconnect();
          self.error(err, res);
        });
      }
    });
  };

  this.getIndexes = function(req, res) {
    var db = isNaN(req.params.db) ? req.params.db : parseInt(req.params.db);
    var table = isNaN(req.params.table) ? req.params.table
        : parseInt(req.params.table);

    self.connect(req.broker, function(err, conn) {
      if (err) {
        self.error(err, res);
      } else {
        conn.getMetaData().getDatabase(db).then(function(dbMeta) {
          return dbMeta.getTable(table);
        }, function() {
          conn.disconnect();
          res.status(404).send();
        }).then(function(tbl) {
          return tbl.getAllIndexes();
        }, function() {
          conn.disconnect();
          res.status(404).send();
        }).then(function(indexes) {
          conn.disconnect();
          res.status(200).send(indexes);
        }, function(err) {
          conn.disconnect();
          self.error(err, res);
        });
      }
    });
  };

  this.getPk = function(conn, db, table, cb) {
    var tableName = (db + '.' + table).toLowerCase();

    if (pkInfo[tableName]) {
      cb(null, pkInfo[tableName]);
    } else {
      conn.getMetaData().getDatabase(db).then(function(dbMeta) {
        return dbMeta.getTable(table);
      }).then(function(tbl) {
        return tbl.getPk();
      }).then(function(pk) {
        pkInfo[tableName] = pk.fields;
        cb(null, pkInfo[tableName]);
      })['catch'](function(err) {
        cb(err);
      });
    }
  };

  this.getPkFilter = function(pk, val) {
    if (typeof val === 'string') {
      if (val.length > 0 && val.charAt(val.length - 1) === '/') {
        val = val.substring(0, val.length - 1);
      }

      val = val.split('/');
    }

    if (pk instanceof Array && val instanceof Array && pk.length > 0
        && pk.length === val.length) {
      var crit = [];
      for ( var i in pk) {
        var fld = pk[i];
        crit.push(f.eq(fld.name, val[fld.fld_pos - 1]));
      }

      return f.and(crit);
    } else {
      throw new Error('Invalid primary key values.');
    }
  };

  this._getQuery = function(conn, req, pk) {
    var table = req.params.db + '.' + req.params.table;
    var pkFilter = null;

    if (pk) {
      pkFilter = self.getPkFilter(pk, req.params[0]);
    }

    var qry = conn.query.select(table).fields();
    var filter = req.query.filter || {};

    if (typeof filter === 'string') {
      try {
        filter = JSON.parse(filter);

        if (filter.where && pkFilter) {
          pkFilter.and.push(filter.where);
          filter.where = pkFilter;
        }
      } catch (err) {
        throw new Error('Invalid query filter.');
      }
    }

    if (pkFilter && !filter.where) {
      filter.where = pkFilter;
    }

    if (typeof filter === 'object') {
      for ( var key in filter) {
        if (typeof qry[key] === 'function') {
          qry[key](filter[key]);
        } else {
          throw new Error('Invalid filter option: ' + key);
        }
      }
    }

    return qry;
  };

  this.getQuery = function(conn, req, cb) {
    var pkVal = req.params[0];

    if (pkVal) {
      self.getPk(conn, req.params.db, req.params.table, function(err, pk) {
        if (err) {
          cb(err);
        } else {
          try {
            cb(null, self._getQuery(conn, req, pk));
          } catch (e) {
            cb(e);
          }
        }
      });
    } else {
      try {
        cb(null, self._getQuery(conn, req));
      } catch (err) {
        cb(err);
      }
    }
  };

  this.doSelect = function(req, res) {
    self.connect(req.broker, function(err, conn) {
      if (err) {
        self.error(err, res);
      } else {
        self.getQuery(conn, req, function(err, qry) {
          if (err) {
            conn.disconnect();
            self.error(err, res);
          } else {
            qry.all().then(function(rows) {
              conn.disconnect();
              switch (rows.length) {
              case 0:
                res.status(404).send();
                break;
              case 1:
                res.status(200).send(rows[0]);
                break;
              default:
                res.status(200).send(rows);
              }
            })['catch'](function(err) {
              conn.disconnect();
              self.error(err, res);
            });
          }
        });
      }
    });
  };

  this.doCreate = function(req, res) {
    self.connect(req.broker, function(err, conn) {
      if (err) {
        self.error(err, res);
      } else {
        try {
          var table = req.params.db + '.' + req.params.table;

          conn.query.insert(table).set(req.body).fetch().then(function(row) {
            conn.disconnect();
            res.status(200).send(row);
          })['catch'](function(err) {
            conn.disconnect();
            self.error(err, res);
          });
        } catch (e) {
          conn.disconnect();
          self.error(e, res);
        }
      }
    });
  };

  this._update = function(conn, table, filter, data, res) {

    try {
      conn.query.update(table).where(filter).set(data).fetch().then(
          function(rows) {
            conn.disconnect();
            switch (rows.length) {
            case 0:
              res.status(404).send();
              break;
            case 1:
              res.status(200).send(rows[0]);
              break;
            default:
              res.status(200).send(rows);
            }
          })['catch'](function(err) {
        conn.disconnect();
        self.error(err, res);
      });
    } catch (e) {
      conn.disconnect();
      self.error(e, res);
    }

  };

  this.doUpdate = function(req, res) {
    self.connect(req.broker, function(err, conn) {
      if (err) {
        self.error(err, res);
      } else {
        var db = req.params.db;
        var table = req.params.table;
        self.getPk(conn, db, table, function(err, pk) {
          if (err) {
            self.error(err, res);
          } else {
            self._update(conn, db + '.' + table, self.getPkFilter(pk,
                req.params[0]), req.body);
          }
        });
      }
    });
  };

  this.doUpdateByRowid = function(req, res) {
    self.connect(req.broker, function(err, conn) {
      if (err) {
        self.error(err, res);
      } else {
        var table = req.params.db + '.' + req.params.table;
        var filter = f.rowid(table, req.params.id);

        self._update(conn, db + '.' + table, filter, req.body);
      }
    });
  };

  this._delete = function(conn, table, filter, res) {
    try {
      conn.query.destroy(table).where(filter).go().then(function(result) {
        conn.disconnect();
        if (result === 0) {
          res.status(404).send();
        } else {
          res.status(200).send({
            num : result
          });
        }
      })['catch'](function(err) {
        conn.disconnect();
        self.error(err, res);
      });
    } catch (e) {
      conn.disconnect();
      self.error(e, res);
    }
  };

  this.doDelete = function(req, res) {
    self.connect(req.broker, function(err, conn) {
      if (err) {
        self.error(err, res);
      } else {
        var db = req.params.db;
        var table = req.params.table;
        self.getPk(conn, db, table, function(err, pk) {
          if (err) {
            self.error(err, res);
          } else {
            self._delete(conn, db + '.' + table, self.getPkFilter(pk,
                req.params[0]), res);
          }
        });
      }
    });
  };

  this.doDeleteByRowid = function(req, res) {
    self.connect(req.broker, function(err, conn) {
      if (err) {
        self.error(err, res);
      } else {

        var table = req.params.db + '.' + req.params.table;
        var filter = f.rowid(table, req.params.id);

        self._delete(conn, table, filter, res);

      }
    });
  };

  this.doCount = function(req, res) {
    self.connect(req.broker, function(err, conn) {
      if (err) {
        self.error(err, res);
      } else {
        try {
          var qry = self._getQuery(conn, req);
          qry.count().then(function(rows) {
            conn.disconnect();
            res.status(200).send({
              num : rows
            });
          })['catch'](function(err) {
            conn.disconnect();
            self.error(err, res);
          });
        } catch (e) {
          conn.disconnect();
          self.error(e, res);
        }
      }
    });
  };

  this.init = function(config, router) {

    if (!router || !router.__app || typeof router.__app.require !== 'function') {
      throw new Error('Invalid Akera web service router.');
    }

    config = config || {};
    akeraApp = router.__app;
    config.route = akeraApp.getRoute(config.route || '/rest/crud/');

    router.get(config.route + 'meta', self.getDatabases);
    router.get(config.route + 'meta/:db', self.getTables);
    router.get(config.route + 'meta/:db/:table', self.getFields);
    router.get(config.route + 'meta/:db/:table/index(es)?', self.getIndexes);

    router.get(config.route + ':db/:table', self.doSelect);
    router.get(config.route + ':db/:table/count', self.doCount);
    router.get(config.route + ':db/:table/*', self.doSelect);
    router.post(config.route + ':db/:table', self.doCreate);
    router.put(config.route + ':db/:table/rowid/:id', self.doUpdateByRowid);
    router.put(config.route + ':db/:table/*', self.doUpdate);
    router['delete'](config.route + ':db/:table/rowid/:id',
        self.doDeleteByRowid);
    router['delete'](config.route + ':db/:table/*', self.doDelete);
  };

  if (akeraWebApp !== undefined) {
    throw new Error(
        'Rest File service can only be mounted at the broker level.');
  }
}

AkeraRestCrud.init = function(config, router) {
  var restCrud = new AkeraRestCrud();
  restCrud.init(config, router);
};