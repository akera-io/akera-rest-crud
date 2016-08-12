var akera = require('akera-api');
var async = require('async');
var CacheManager = require('./cache.js');
var cacheMngr = new CacheManager();
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
        if (cacheMngr.rootLoaded()) {
          root.services = cacheMngr.getAllServices();
          return resolve(root);
        } else {
          self.akeraMetadata.getDatabases(broker, true).then(
            function(structure) {
              Object.keys(structure).forEach(
                function(dbName) {
                  root.services.push(_getDatabaseService(dbName,
                    structure[dbName], asDataset));
                  cacheMngr.storeService(service);
                });
              cacheMngr.rootLoaded(true);
              resolve(root);
            }, reject);
        }
      } else if (!tableName) {
        var s = cacheMngr.getService(dbName);
        if (s && (cacheMngr.rootLoaded() || cacheMngr.isSvcFullyLoaded(s))) {
          root.services.push(s);
          return resolve(root);
        } else {
          self.akeraMetadata.getTables(broker, dbName, true).then(
            function(tables) {
              var service = _getDatabaseService(dbName, tables, asDataset);
              root.services.push(sv);
              cacheMngr.storeService(sv, true);
              resolve(service);
            }, reject);
        }
      } else {
        var srvTable = cacheMngr.getService(dbName, tableName);
        if (srvTable) {
          root.services.push(srvTable);
          return resolve(root);
        } else {
          self.akeraMetadata.getTable(dbName, tableName).then(
            function(tableMeta) {
              var service = _getDatabseService(dbName, tableMeta, asDataset,
                true);
              root.services.push(service);
              cacheMngr.storeService(service);
              resolve(root);
            }, reject);
        }
      }
    });
  };
}

function getRootNode() {
  return {
    version : '1.3',
    lastModified : cacheMngr.lastModified,
    services : []
  };
}

function _getDatabaseService(dbName, tables, asDataset, singleTable) {
  var service = {
    name : dbName,
    address : '\/' + dbName,
    useRequest : true,
    resources : []
  };

  if (singleTable) {
    service.resources.push(_getTableResource(tables, asDataset));
    return service;
  }

  Object.keys(tables).forEach(
    function(tableName) {
      service.resources.push(_getTableResource(tableName, tables[tableName],
        asDataset));
    });

  return service;
}

function _getTableResource(tableName, tableMeta, asDataset) {
  var suffix = _getPkHttpSuffix(tableMeta);
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

  Object.keys(tableMeta.fields, function(fieldName) {
    var field = tableMeta.fields[fieldName];
    var prop = {
      type : field.extent > 1 ? 'array' : jsonAblMap[field.type],
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
