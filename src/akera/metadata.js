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

  this.getCatalog = function(dbName, tableName, asDataset, broker, cb) {
    var root = getRootNode();

    if (!tableName && !dbName) {
      if (cacheMngr.rootLoaded()) {
        root.services = cacheMngr.getAllServices();
        return cb(null,root);
      } else {
        akera.connect(broker).then(function(conn) {
          conn.getMetaData().allDatabases().then(function(dbs) {
            async.forEach(dbs, function(db, acb) {
              getDatabaseService(db, null, asDataset, function(err, service) {
                root.services.push(service);
                cacheMngr.storeService(service);
                acb(err);
              });
            }, function(err) {
              cacheMngr.rootLoaded(true);
              conn.disconnect().then(function() {
                if (err) {
                  cb(err);
                } else {
                  cb(null,root);
                }
              });
            });
          },cb);
        }, cb);
      }

      return;
    }

    if (!tableName) {
      var s = cacheMngr.getService(dbName);
      if (s && (cacheMngr.rootLoaded() || cacheMngr.isSvcFullyLoaded(s))) {
        root.services.push(s);
        cb(null,root);
      } else {
        akera.connect(broker).then(function(conn) {
          conn.getMetaData().getDatabase(dbName).then(function(db) {
            getDatabaseService(db, null, asDataset, function(err, sv) {
              if (err) {
                cb(err);
              } else {
                root.services.push(sv);
                cacheMngr.storeService(sv, true);
                conn.disconnect().then(function() {
                  cb(null,root);
                });
              }
            });
          }, cb);
        }, cb);
      }
    } else {
      var srvTable = cacheMngr.getService(dbName, tableName);

      if (srvTable) {
        root.services.push(srvTable);
        return cb(null,root);
      } else {
        akera.connect(broker).then(function(conn) {
          conn.getMetaData().getDatabase(dbName).then(function(db) {
            getDatabaseService(db, tableName, asDataset, function(err, sv) {
              if (err) {
                return cb(err);
              } else {
                root.services.push(sv);
                cacheMngr.storeService(sv);
                conn.disconnect().then(function() {
                  cb(null,root);
                });
              }
            });
          }, cb);
        }, cb);
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

function getDatabaseService(db, table, asDataset, cb) {
  var service = {
    name : db.getName(),
    address : '\/' + db.getName(),
    useRequest : true,
    resources : []
  };

  if (table && typeof (table) === 'string') {
    db.getTable(table).then(function(tblMeta) {
      getTableResource(tblMeta, asDataset, function(err, r) {
        service.resources.push(r);
        cb(null, service);
      }, cb);
    }, cb);
  } else {
    db.allTables().then(function(tbls) {
      async.forEach(tbls, function(table, bcb) {
        getTableResource(table, asDataset, function(err, r) {
          service.resources.push(r);
          bcb(err);
        });
      }, function() {
        cb(null, service);
      });
    }, cb);
  }
}

function getTableResource(table, asDataset, cb) {
  getPkHttpSuffix(
    table, asDataset,
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
            path : '?jsdoFilter={filter}&sort={sort}&skip={skip}&top={top}',
            type : 'read',
            verb : 'get',
            params : []
          }, {
            path : '',
            useBeforeImage : false,
            type : 'create',
            verb : 'post',
            params : [ {
              name : asDataset ? 'ds' + table.getName() : table.getName(),
              type : 'REQUEST_BODY,RESPONSE_BODY'
            } ]
          }, {
            path : suffix,
            useBeforeImage : asDataset,
            type : 'update',
            verb : 'put',
            params : [ {
              name : asDataset ? 'ds' + table.getName() : table.getName(),
              type : 'REQUEST_BODY,RESPONSE_BODY'
            } ]
          }, {
            path : suffix,
            useBeforeImage : false,
            type : 'delete',
            verb : 'delete',
            params : []
          }, {
            path : '/count?filter={filter}',
            name : 'count',
            type : 'invoke',
            verb : 'get',
            useBeforeImage : false,
            params : [ {
              name : 'num',
              type : 'RESPONSE_BODY',
              xType : 'number'
            } ]
          } ]
        };

        if (asDataset === true) {
          resource.schema.properties['ds' + table.getName()] = {
            type : 'object',
            additionalProperties : false,
            properties : {
             
            }
          };
          resource.schema.properties['ds' + table.getName()].properties['tt'
            + table.getName()] = {
            type : 'array',
            items : {
              additionalProperties : false,
              properties : {
                _id : {
                  type : 'string'
                },
                errorString : {
                  type : 'string'
                }
              }
            }
          };
        } else {
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
        }
        var tableNode = asDataset ? resource.schema.properties['ds'
          + table.getName()].properties['tt' + table.getName()]
          : resource.schema.properties[table.getName()];
          console.log(resource.schema);
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
                    tableNode.items.properties[field.name] = prop;
                    ccb();
                  },
                  function() {
                    table
                      .getPk()
                      .then(
                        function(pkFields) {
                          tableNode.primaryKey = pkFields.fields
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

function getPkHttpSuffix(table, asDataset, cb) {
  if (asDataset === true) {
    return cb(null, '');
  }
  table.getPk().then(function(pks) {
    var httpSuffix = '/';
    pks.fields.forEach(function(pk) {
      httpSuffix += '{' + pk.name + '}/';
    });
    console.log(httpSuffix);
    cb(null, httpSuffix);
  }, cb);
}

module.exports = JSDOCatalog;
