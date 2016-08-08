var akera = require('akera-api');
var async = require('async');
var CacheManager = require('./cache.js');
var cacheMngr = new CacheManager();

var jsonAblMap = {
  character : 'string',
  blob : 'string',
  integer : 'number',
  decimal : 'number',
  logical : 'boolean',
  date : 'date',
  datetime : 'date',
  datetimetz : 'date'
};

function JSDOCatalog() {

  this.getCatalog = function(dbName, tableName, broker, res) {
    var root = getRootNode();

    if (!tableName && !dbName) {
      if (cacheMngr.rootLoaded()) {
        root.services = cacheMngr.getAllServices();
        return res.status(200).json(root);
      } else {
        akera.connect(broker).then(function(conn) {
          conn.getMetaData().allDatabases().then(function(dbs) {
            async.forEach(dbs, function(db, acb) {
              getDatabaseService(db, null, function(err, service) {
                root.services.push(service);
                cacheMngr.storeService(service);
                acb(err);
              });
            }, function(err) {
              cacheMngr.rootLoaded(true);
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

      return;
    }

    if (!tableName) {
      var s = cacheMngr.getService(dbName);
      if (s && (cacheMngr.rootLoaded() || cacheMngr.isSvcFullyLoaded(s))) {
        root.services.push(s);
        return res.status(200).json(root);
      } else {
        akera.connect(broker).then(function(conn) {
          conn.getMetaData().getDatabase(dbName).then(function(db) {
            getDatabaseService(db, null, function(err, sv) {
              if (err) {
                error(err, res);
              } else {
                root.services.push(sv);
                cacheMngr.storeService(sv, true);
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
    } else {
      var srvTable = cacheMngr.getService(dbName, tableName);

      if (srvTable) {
        root.services.push(srvTable);
        return res.status(200).json(root);
      } else {
        akera.connect(broker).then(function(conn) {
          conn.getMetaData().getDatabase(dbName).then(function(db) {
            getDatabaseService(db, tableName, function(err, sv) {
              if (err) {
                return error(err, res);
              } else {
                root.services.push(sv);
                cacheMngr.storeService(sv);
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
    version : '1.3',
    lastModified : cacheMngr.lastModified,
    services : []
  };
}

function getDatabaseService(db, table, cb) {
  var service = {
    name : db.getName(),
    address : '\/' + db.getName(),
    useRequest : true,
    resources : []
  };

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
  getPkHttpSuffix(
    table,
    function(err, suffix) {
      if (err) {
        return cb(err);
      } else {
        var resource = {
          name : table.getName(),
          path : '\/' + table.getName(),
          displayName : table.getName(),
          schema : {
            type : 'object',
            additionalProperties : false,
            properties : {

            }
          },
          operations : [ {
            path : '?filter={filter}',
            type : 'read',
            verb : 'get',
            params : []
          }, {
            path : '',
            useBeforeImage : false,
            type : 'create',
            verb : 'post',
            params : [ {
              name : table.getName(),
              type : 'REQUEST_BODY,RESPONSE_BODY'
            } ]
          }, {
            path : suffix,
            useBeforeImage : false,
            type : 'update',
            verb : 'put',
            params : [ {
              name : table.getName(),
              type : 'REQUEST_BODY,RESPONSE_BODY'
            } ]
          }, {
            path : '',
            useBeforeImage : false,
            type : 'delete',
            verb : 'delete',
            params : [ {
              name : table.getName(),
              type : 'REQUEST_BODY,RESPONSE_BODY'
            } ]
          } ]
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
                      type : field.extent > 1 ? 'array'
                        : jsonAblMap[field.type],
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

    });

}

function getPkHttpSuffix(table, cb) {
  table.getPk().then(function(pks) {
    var httpSuffix = '/';
    pks.fields.forEach(function(pk) {
      httpSuffix += '{' + pk.name + '}/';
    });
    cb(null, httpSuffix);
  }, cb);
}

function error(err, res) {
  res.status(500).json(err instanceof Error ? {
    message : err.message,
    stack : err.stack
  } : err);
}

module.exports = JSDOCatalog;
