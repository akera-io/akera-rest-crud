var akera = require('akera-api');
var async = require('async');
var rsvp = require('rsvp');

function AkeraMetaData() {
  this.cache = {};

}

AkeraMetaData.prototype.getDatabases = function(broker, fullLoad) {
  var self = this;

  return new rsvp.Promise(function(resolve, reject) {
    if (!broker || !broker.alias)
      reject(new Error('Invalid broker.'));

    var brokerInfo = self.cache[broker.alias];

    if (brokerInfo && (fullLoad !== true || brokerInfo.fullyLoaded === true)) {
      return resolve(brokerInfo.db);
    }

    akera.connect(broker).then(function(conn) {
      self._conn = conn;

      return conn.getMetaData().allDatabases();
    }).then(function(dbs) {
      brokerInfo = brokerInfo || {
        fullyLoaded : fullLoad === true
      };
      brokerInfo.db = brokerInfo.db || {};

      if (fullLoad === true) {
        async.forEach(dbs, function(dbMeta, cb) {
          var dbName = dbMeta.getLname().toLowerCase();
          var dbInfo = brokerInfo.db[dbName] || {};

          // database info already loaded
          if (dbInfo && dbInfo.fullyLoaded)
            return cb();

          self.loadDatabase(dbMeta, fullLoad).then(function(info) {
            brokerInfo.db[dbName] = info;
            cb();
          }, cb);
        }, function(err) {
          if (err)
            return self.disconnect(reject, err);

          self.cache[broker.alias] = brokerInfo;
          self.disconnect(resolve, brokerInfo.db);
        });

      } else {
        dbs.forEach(function(db) {
          var dbName = db.getLname().toLowerCase();

          if (!brokerInfo.db[dbName])
            brokerInfo.db[dbName] = {};
        });
        self.cache[broker.alias] = brokerInfo;
        self.disconnect(resolve, brokerInfo.db);
      }
    })['catch'](function(err) {
      self.disconnect(reject, err);
    });
  });
};

AkeraMetaData.prototype.getDatabase = function(broker, db, fullLoad) {
  var self = this;

  return new rsvp.Promise(
    function(resolve, reject) {
      if (!broker || !broker.alias)
        reject(new Error('Invalid broker.'));

      if (typeof db !== 'number'
        && (typeof db !== 'string' || db.trim().length === 0))
        reject(new Error('Invalid database name.'));

      if (typeof db === 'string')
        db = db.trim().toLowerCase();

      var brokerInfo = self.cache[broker.alias];
      var dbInfo = brokerInfo && brokerInfo.db && brokerInfo.db[db];

      if (dbInfo
        && (dbInfo.fullyLoaded === true || (!fullLoad && dbInfo.numTables === dbInfo.loadedTables)))
      {
        return resolve(dbInfo);
      }

      akera.connect(broker).then(function(conn) {
        self._conn = conn;
        return conn.getMetaData().getDatabase(db);
      }).then(function(dbMeta) {
        db = dbMeta.getLname().trim().toLowerCase();
        return self.loadDatabase(dbMeta, fullLoad);
      }).then(function(info) {
        if (!brokerInfo) {
          brokerInfo = {};
          self.cache[broker.alias] = brokerInfo;
        }

        brokerInfo.db = brokerInfo.db || {};
        brokerInfo.db[db] = info;

        self.disconnect(resolve, info);
      })['catch'](function(err) {
        self.disconnect(reject, err);
      });
    });
};

AkeraMetaData.prototype.getTables = function(broker, db, fullLoad) {
  var self = this;

  return new rsvp.Promise(function(resolve, reject) {
    if (!broker || !broker.alias)
      reject(new Error('Invalid broker.'));

    if (typeof db !== 'number'
      && (typeof db !== 'string' || db.trim().length === 0))
      reject(new Error('Invalid database name.'));

    if (typeof db === 'string')
      db = db.trim().toLowerCase();

    var brokerInfo = self.cache[broker.alias];
    var dbInfo = brokerInfo && brokerInfo.db && brokerInfo.db[db];

    if (dbInfo && (fullLoad !== true || dbInfo.fullyLoaded === true)) {
      return resolve(dbInfo.table);
    }

    akera.connect(broker).then(function(conn) {
      self._conn = conn;
      return conn.getMetaData().getDatabase(db);
    }).then(function(dbMeta) {
      db = dbMeta.getLname().trim().toLowerCase();
      return self.loadDatabase(dbMeta, fullLoad);
    }).then(function(info) {
      if (!brokerInfo) {
        brokerInfo = {};
        brokerInfo.db = {};

        self.cache[broker.alias] = brokerInfo;
      }

      brokerInfo.db[db] = info;

      self.disconnect(resolve, info.table);

    })['catch'](function(err) {
      self.disconnect(reject, err);
    });
  });

};

AkeraMetaData.prototype.getTable = function(broker, db, table) {
  var self = this;

  return new rsvp.Promise(function(resolve, reject) {
    if (!broker || !broker.alias)
      reject(new Error('Invalid broker.'));

    if (typeof db !== 'number'
      && (typeof db !== 'string' || db.trim().length === 0))
      reject(new Error('Invalid database name.'));

    if (typeof db === 'string')
      db = db.trim().toLowerCase();

    if (typeof table !== 'number'
      && (typeof table !== 'string' || table.trim().length === 0))
      reject(new Error('Invalid table name.'));

    if (typeof table === 'string')
      table = table.trim().toLowerCase();

    var brokerInfo = self.cache[broker.alias];
    var dbInfo = brokerInfo && brokerInfo.db && brokerInfo.db[db];
    var tableInfo = dbInfo && dbInfo.table && dbInfo.table[table];
    var numTables = dbInfo && dbInfo.numTables;
    var databaseMeta = null;

    if (tableInfo) {
      return resolve(tableInfo);
    }

    akera.connect(broker).then(function(conn) {
      self._conn = conn;
      return conn.getMetaData().getDatabase(db);
    }).then(function(dbMeta) {
      if (!numTables)
        databaseMeta = dbMeta;

      db = dbMeta.getLname().trim().toLowerCase();

      return dbMeta.getTable(table);
    }).then(function(tableMeta) {
      table = tableMeta.getName().trim().toLowerCase();

      return self.loadTable(tableMeta);
    }).then(function(info) {
      if (!dbInfo) {
        if (!brokerInfo) {
          brokerInfo = {};
          brokerInfo.db = {};

          self.cache[broker.alias] = brokerInfo;
        }

        brokerInfo.db[db] = {};
        dbInfo = brokerInfo.db[db];
      }

      dbInfo.table = dbInfo.table || {};
      dbInfo.table[table] = info;

      dbInfo.loadedTables = dbInfo.loadedTables || 0;
      dbInfo.loadedTables++;

      if (!numTables) {
        databaseMeta.getNumTables().then(function(num) {
          dbInfo.numTables = num;
          self.disconnect(resolve, info);
        }, function(err) {
          self.disconnect(reject, err);
        });
      } else {
        self.disconnect(resolve, info);
      }
    })['catch'](function(err) {
      self.disconnect(reject, err);
    });
  });
};

AkeraMetaData.prototype.disconnect = function(cb, data) {
  if (!this._conn)
    return cb && cb(data);

  this._conn.disconnect().then(function() {
    delete this._conn;
    return cb && cb(data);
  }, function() {
    delete this._conn;
    return cb && cb(data);
  });
}

AkeraMetaData.prototype.loadDatabase = function(dbMeta, fullLoad) {
  var self = this;

  return new rsvp.Promise(function(resolve, reject) {
    try {
      dbMeta.allTables().then(function(tbs) {
        var dbInfo = {
          fullyLoaded : fullLoad === true
        };

        dbInfo.table = {};

        if (fullLoad !== true) {
          dbInfo.numTables = tbs.length;
          dbInfo.loadedTables = dbInfo.numTables;

          tbs.forEach(function(tblMeta) {
            dbInfo.table[tblMeta.getName().toLowerCase()] = {};
          });
          return resolve(dbInfo);
        }

        async.forEach(tbs, function(tblMeta, cb) {
          self.loadTable(tblMeta).then(function(tableInfo) {
            dbInfo.table[tblMeta.getName().toLowerCase()] = tableInfo;
            cb();
          }, cb);
        }, function(err) {
          if (err)
            return reject(err);

          resolve(dbInfo);
        });

      }, reject);
    } catch (err) {
      reject(err);
    }
  });

};

AkeraMetaData.prototype.loadTable = function(tblMeta) {
  return new rsvp.Promise(function(resolve, reject) {
    try {
      var tableInfo = {};

      tblMeta.getAllFields().then(function(fields) {
        tableInfo.fields = {};

        fields.forEach(function(field) {
          tableInfo.fields[field.name] = field;
          delete field.name;
        });

        // next get indexes
        return tblMeta.getAllIndexes();
      }).then(function(indexes) {
        tableInfo.indexes = {};

        indexes.forEach(function(index) {
          tableInfo.indexes[index.name] = index;
          delete index.name;
        });

        // last step get pk info
        return tblMeta.getPk();
      }).then(function(pk) {
        if (pk && pk.fields)
          tableInfo.pk = pk.fields.sort(function(a, b) {
            return a.fld_pos - b.fld_pos;
          }).map(function(fld) {
            return fld.name;
          });
        resolve(tableInfo);
      })['catch'](reject);
    } catch (err) {
      reject(err);
    }
  });
};

module.exports = AkeraMetaData;
