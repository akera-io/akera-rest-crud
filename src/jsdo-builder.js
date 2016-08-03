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

    var root = {
      version: '1.0',
      lastModified: new Date().toString(),
      services: []
    };
    if (!tableName) {
      if (cacheMgr.rootLoaded()) {
        root.services = cacheMgr.getAllServices();
        return res.status(200).json(root);
      } else {
        var broker = req.broker;
      akera
        .connect(broker)
        .then(
          function(conn) {
            conn
              .getMetaData()
              .allDatabases()
              .then(
                function(dbs) {
                  async
                    .forEach(
                      dbs,
                      function(db, acb) {
                        var service = {
                          name : db.getName() + 'Service',
                          address : '\/' + db.getName(),
                          useRequest : true,
                          resources : []
                        };
                        db
                          .allTables()
                          .then(
                            function(tbls) {
                              async
                                .forEach(
                                  tbls,
                                  function(table, bcb) {
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
                                                resource.schema.properties[table
                                                  .getName()].items.properties[field.name] = prop;
                                                table
                                                  .getPk()
                                                  .then(
                                                    function(pkFields) {
                                                      resource.schema.properties[table
                                                        .getName()].primaryKey = pkFields.fields
                                                        .map(function(pkFld) {
                                                          return pkFld.name;
                                                        });
                                                      ccb();
                                                    }, ccb);
                                              }, function() {
                                                service.resources
                                                  .push(resource);
                                                bcb();
                                              });
                                        }, bcb);
                                  }, function() {
                                    root.services.push(service);
                                    cacheMgr.storeService(service);
                                    acb();
                                  });
                            }, acb);
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
    }
  };
}

function error(err, res) {
  res.status(500).json(err);
}
module.exports = JSDOBuilder;
