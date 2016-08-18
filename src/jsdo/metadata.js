var rsvp = require('rsvp');

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

function JSDOCatalog(akeraMetadata) {
  var self = this;

  this.akeraMetadata = akeraMetadata;

  this.getCatalog = function(dbName, tableName, asDataset, broker) {
    return new rsvp.Promise(function(resolve, reject) {

      var root = getRootNode();

      if (!tableName && !dbName) {
        self.akeraMetadata.getDatabases(broker, true).then(
          function(structure) {
            Object.keys(structure).forEach(
              function(dbName) {
                try {
                  root.services.push(_getDatabaseService(dbName,
                    structure[dbName].table, asDataset));
                } catch (err) {
                  reject(err);
                }
              });
            resolve(root);
          }, reject);
      } else if (!tableName) {
        self.akeraMetadata.getTables(broker, dbName, true).then(
          function(tables) {
            try {
              root.services
                .push(_getDatabaseService(dbName, tables, asDataset));
              resolve(root);
            } catch (err) {
              reject(err);
            }
          }, reject);
      } else {
        self.akeraMetadata.getTable(broker, dbName, tableName).then(
          function(tableMeta) {
            try {
              var tables = {};
              tables[tableName] = tableMeta;
              root.services
                .push(_getDatabaseService(dbName, tables, asDataset));
              resolve(root);
            } catch (err) {
              reject(err);
            }
          }, reject);
      }
    });
  };
}

function getRootNode() {
  return {
    version : '1.3',
    lastModified : new Date().toString(),
    services : []
  };
}

function _getDatabaseService(dbName, tables, asDataset) {
  var service = {
    name : dbName,
    address : '\/' + dbName,
    useRequest : true,
    resources : []
  };

  Object.keys(tables).forEach(
    function(tableName) {
      service.resources.push(_getTableResource(tableName, tables[tableName],
        asDataset));
    });

  return service;
}

function _getTableResource(tableName, tableMeta, asDataset) {
  var suffix = _getPkHttpSuffix(tableMeta, asDataset);
  var resource = {
    name : tableName,
    path : '\/' + tableName,
    displayName : tableName,
    schema : {
      type : 'object',
      additionalProperties : false,
      properties : {

      }
    },
    operations : [ {
      path : '?filter={filter}&sort={sort}&skip={skip}&top={top}',
      type : 'read',
      verb : 'get',
      params : [ {
        name : asDataset ? 'ds' + tableName : tableName,
        type : 'REQUEST_BODY,RESPONSE_BODY'
      } ]
    }, {
      path : '',
      useBeforeImage : false,
      type : 'create',
      verb : 'post',
      params : [ {
        name : asDataset ? 'ds' + tableName : tableName,
        type : 'REQUEST_BODY,RESPONSE_BODY'
      } ]
    }, {
      path : suffix,
      useBeforeImage : asDataset,
      type : 'update',
      verb : 'put',
      params : [ {
        name : asDataset ? 'ds' + tableName : tableName,
        type : 'REQUEST_BODY,RESPONSE_BODY'
      } ]
    }, {
      path : suffix,
      useBeforeImage : asDataset,
      type : 'delete',
      verb : 'delete',
      params : asDataset ? [ {
        name : 'ds' + tableName,
        type : 'REQUEST_BODY'
      } ] : []
    }, {
      path : '/count?filter={filter}',
      name : 'count',
      type : 'invoke',
      verb : 'get',
      useBeforeImage : false,
      params : [ {
        name : 'count',
        type : 'RESPONSE_BODY',
        xType : 'number'
      } ]
    } ]
  };

  if (asDataset === true) {
    resource.schema.properties['ds' + tableName] = {
      type : 'object',
      additionalProperties : false,
      properties : {

      }
    };
    resource.schema.properties['ds' + tableName].properties['tt' + tableName] = {
      type : 'array',
      primaryKey : tableMeta.pk,
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
    resource.schema.properties[tableName] = {
      type : 'array',
      primaryKey : tableMeta.pk,
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

  var tableNode = asDataset ? resource.schema.properties['ds' + tableName].properties['tt'
    + tableName]
    : resource.schema.properties[tableName];

  Object.keys(tableMeta.fields).forEach(function(fieldName) {
    var field = tableMeta.fields[fieldName];
    var prop = {
      type : field.extent > 1 ? 'array' : jsonAblMap[field.type],
      title : field.label,
      format : field.format,
      ablType : field.type.toUpperCase(),
      required : field.mandatory
    };

    if (prop.type === 'array') {
      prop.maxItems = field.extent;
      prop.items = {
        type : prop.type
      };
    }
    tableNode.items.properties[fieldName] = prop;
  });
  return resource;
}

function _getPkHttpSuffix(table, asDataset) {
  if (asDataset === true) {
    return '';
  }
  var suffix = '/';
  table.pk.forEach(function(pkField) {
    suffix += '{' + pkField + '}/';
  });
  return suffix;
}

module.exports = JSDOCatalog;
