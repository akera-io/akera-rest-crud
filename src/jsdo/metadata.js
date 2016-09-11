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

function JSDOCatalog(akeraMetadata, asDataset, sqlSafe) {
  var self = this;

  this.akeraMetadata = akeraMetadata;
  this.asDataset = asDataset !== undefined ? asDataset : true;
  this.sqlSafe = sqlSafe !== undefined ? sqlSafe : false;

  this.getCatalog = function(dbName, tableName, broker) {
    return new rsvp.Promise(function(resolve, reject) {

      var root = getRootNode();

      if (!tableName && !dbName) {
        self.akeraMetadata.getDatabases(broker, true).then(
          function(structure) {
            Object.keys(structure).forEach(
              function(dbName) {
                try {
                  root.services.push(_getDatabaseService(dbName,
                    structure[dbName].table, self.asDataset, self.sqlSafe));
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
              root.services.push(_getDatabaseService(dbName, tables,
                self.asDataset, self.sqlSafe));
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
              root.services.push(_getDatabaseService(dbName, tables,
                self.asDataset, self.sqlSafe));
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

function _getDatabaseService(dbName, tables, asDataset, sqlSafe) {
  var service = {
    name : dbName,
    address : '\/' + dbName,
    useRequest : true,
    resources : []
  };

  Object.keys(tables).forEach(
    function(tableName) {
      service.resources.push(_getTableResource(tableName, tables[tableName],
        asDataset, sqlSafe));
    });

  return service;
}

function _getTableResource(tableName, tableMeta, asDataset, sqlSafe) {
  var paramName = asDataset ? 'ds' + tableName : tableName;
  var xType = asDataset ? 'DATASET' : 'TABLE';
  var sqlName = sqlSafe ? tableMeta.sqlName || tableName : tableName;

  var resource = {
    name : sqlName,
    path : '\/' + tableName,
    displayName : sqlName,
    schema : {
      type : 'object',
      additionalProperties : false,
      properties : {

      }
    },
    operations : [ {
      type : 'read',
      verb : 'get',
      path : '?filter={filter}&sort={sort}&skip={skip}&top={top}',
      useBeforeImage : false,
      params : [ {
        name : paramName,
        type : 'RESPONSE_BODY',
        xType : xType
      } ]
    }, {
      type : 'create',
      verb : 'post',
      path : '',
      useBeforeImage : true,
      params : [ {
        name : paramName,
        type : 'REQUEST_BODY,RESPONSE_BODY',
        xType : xType
      } ]
    }, {
      type : 'update',
      verb : 'put',
      path : '',
      useBeforeImage : true,
      params : [ {
        name : paramName,
        type : 'REQUEST_BODY,RESPONSE_BODY',
        xType : xType
      } ]
    }, {
      type : 'delete',
      verb : 'delete',
      path : '',
      useBeforeImage : true,
      params : [ {
        name : paramName,
        type : 'REQUEST_BODY,RESPONSE_BODY',
        xType : xType
      } ]
    }, {
      name : 'count',
      type : 'invoke',
      verb : 'put',
      path : '/count?filter={filter}',
      useBeforeImage : false,
      params : [ {
        name : 'numRecs',
        type : 'RESPONSE_BODY',
        xType : 'integer'
      } ]
    } ]
  };

  var tableSchema = {
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

  if (tableMeta.pk) {
    tableSchema.primaryKey = sqlSafe ? tableMeta.sqlPk || tableMeta.pk
      : tableMeta.pk;
  }

  if (asDataset === true) {
    resource.schema.properties[paramName] = {
      type : 'object',
      additionalProperties : false,
      properties : {

      }
    };
    resource.schema.properties[paramName].properties[sqlName] = tableSchema;
  } else {
    resource.schema.properties[sqlName] = tableSchema;
  }

  Object.keys(tableMeta.fields).forEach(function(fieldName) {
    var field = tableMeta.fields[fieldName];

    if (sqlSafe)
      fieldName = field.sqlName || fieldName;

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
    tableSchema.items.properties[fieldName] = prop;
  });

  return resource;
}

module.exports = JSDOCatalog;
