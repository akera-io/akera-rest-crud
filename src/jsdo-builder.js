var akera = require('akera-api');
var jsonAblMap = {
  character : 'string',
  blob : 'string',
  integer : 'number',
  decimal : 'number',
  logical : 'boolean',
  date : 'date',
  datetime : 'date',
  datetimetz : 'date',
  extent : 'array'
};
var async = require('async');
var CacheManager = require('./jsdo-cache.js');
var cacheMgr = new CacheManager();

function JSDOBuilder() {

  this.buildCatalog = function(req, res) {
    var tableName = req.params.table;
    var dbName = req.params.db;

    var root = getRootNode();

    if (!tableName && !dbName) {
      if (cacheMgr.rootLoaded()) {
        root.services = cacheMgr.getAllServices();
        return res.status(200).json(root);
      } else {
        var broker = req.broker;
        akera.connect(broker).then(function(conn) {
          conn.getMetaData().allDatabases().then(function(dbs) {
            async.forEach(dbs, function(db, acb) {
              getDatabaseService(db, null, function(err, service) {
                root.services.push(service);
                cacheMgr.storeService(service);
                acb(err);
              });
            }, function(err) {
              cacheMgr.rootLoaded(true);
              conn.disconnect().then(function() {
                if (err) {
                  error(err, res);
                } else {
                  res.status(200).json(root);
                }
              });
            });
          }, function(err) {
            error(err, res);
          });
        }, function(err) {
          error(err, res);
        });
      }
    } else if (dbName && !tableName) {
      console.log(dbName);
      var s = cacheMgr.getService(dbName + 'Service');
      if (s && (cacheMgr.rootLoaded() || cacheMgr.isSvcFullyLoaded(s))) {
        root.services.push(s);
        return res.status(200).json(root);
      } else {
        console.log('service %s is not cached or not fully loaded. loading from db', dbName + 'Service');
        akera.connect(req.broker).then(function(conn) {
          console.log('connected');
          conn.getMetaData().getDatabase(dbName).then(function(db) {
            console.log('got database meta for %s', dbName);
            getDatabaseService(db, null, function(err, sv) {
              console.log('got db service %s', sv.name);
              console.log('error %s', err);
              if (err) {
                error(err, res);
              } else {
                root.services.push(sv);
                cacheMgr.storeService(sv, true);
                console.log('stored service %s', sv.name);
                conn.disconnect().then(function() {
                  console.log('disconnected');
                  res.status(200).json(root);
                });
              }
            });
          }, function(err) {
            error(err, res);
          });
        }, function(err) {
          error(err, res);
        });
      }
    } else {
      var resource = cacheMgr.getTableResource(dbName + 'Service', tableName);
      if (resource) {
        root.services.push(cacheMgr.getService(dbName + 'Service', tableName));
        return res.status(200).json(root);
      } else {
        akera.connect(req.broker).then(function(conn) {
          conn.getMetaData().getDatabase(dbName).then(function(db) {
            getDatabaseService(db, tableName, function(err, sv) {
              if (err) {
                return error(err, res);
              } else {
                root.services.push(sv);
                cacheMgr.storeService(sv);
                conn.disconnect().then(function() {
                  res.status(200).json(root);
                });
              }
            });
          }, function(err) {
            error(err, res);
          });
        }, function(err) {
          error(err, res);
        });
      }
    }
  };
}

function getRootNode() {
  return {
    version : '1.0',
    lastModified : new Date().toString(),
    services : []
  };
}

function getDatabaseService(db, table, cb) {
  var service = {
    name : db.getName() + 'Service',
    address : '\/' + db.getName(),
    useRequest : true,
    resources : []
  };
  console.log(table);
  if (table && typeof (table) === 'string') {
    db.getTable(table).then(function(tblMeta) {
      getTableResource(tblMeta, function(err, r) {
        service.resources.push(r);
        cb(null, service);
      }, cb);
    }, cb);
  } else {
    db.allTables().then(function(tbls) {
      async.forEach(tbls, function(table, bcb) {
        getTableResource(table, function(err, r) {
          service.resources.push(r);
          bcb(err);
        });
      }, function() {
        cb(null, service);
      });
    }, cb);
  }
}

function getTableResource(table, cb) {
  var resource = {
    name : table.getName(),
    path : '\/' + table.getName(),
    displayName : table.getName(),
    schema : {
      type : 'object',
      additionalProperties : false,
      properties : {

      }
    }
  };

  resource.schema.properties[table.getName()] = {
    type : 'array',
    items : {
      additionalProperties : false,
      properties : {
        _id : {
          type : 'string'
        },
        _errorString : {
          type : 'string'
        }
      }
    }
  };

  table
    .getAllFields()
    .then(
      function(fields) {
        async
          .forEach(
            fields,
            function(field, ccb) {
              var prop = {
                type : jsonAblMap[field.type],
                title : field.label,
                format : field.format,
                ablType : field.type,
                required : field.mandatory
              };
              if (prop.type === 'array') {
                prop.maxItems = field.extent;
                prop.items = {
                  type : prop.type
                };
              }
              resource.schema.properties[table.getName()].items.properties[field.name] = prop;
              ccb();
            },
            function() {
              table
                .getPk()
                .then(
                  function(pkFields) {
                    resource.schema.properties[table.getName()].primaryKey = pkFields.fields
                      .map(function(pkFld) {
                        return pkFld.name;
                      });
                    cb(null, resource);
                  }, cb);
            });
      }, cb);
}

function error(err, res) {
  res.status(500).json(err instanceof Error ? {
    message : err.message,
    stack : err.stack
  } : err);
}

module.exports = JSDOBuilder;
