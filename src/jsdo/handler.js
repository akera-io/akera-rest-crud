var JSDOCatalog = require('./metadata.js');
var metadata = new JSDOCatalog();

/*
 * var operatorMap = { neq : 'ne', gte : 'ge', lte : 'le', contains : 'like' };
 */
function JSDOHandler(restHandler) {

  var self = this;

  this.restHandler = restHandler;

  this.getCatalog = function(req, res) {
    var tableName = req.params.table;
    var dbName = req.params.db;

    metadata.getCatalog(dbName, tableName, req.broker, res);
  };

  this.doSelect = function(req, res) {
    if (!req.query.jsdoFilter) {
      return self.restHandler.doSelect(req, res);
    }
    var restFilter = self.filter.fromKendo(req.query.jsdoFilter);
    console.log(JSON.stringify(restFilter, null, '\t'));
    req.query.filter = JSON.stringify(restFilter);
    self.restHandler.doSelect(req, res);
  };

  this.filter = {
    fromKendo : function(kendoFilter) {
      return {
        where : convertKendoFilter(kendoFilter)
      };
    }
  };

}

function getClauseFromKendo(flt) {
  var clause = {};
  switch (flt.operator) {
    case 'eq':
      clause[flt.field] = flt.value;
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
        like : '*' + flt.value + '*'
      };
      break;
    case 'startswith':
      clause[flt.field] = {
        like : flt.value + '*'
      };
      break;
    case 'endswith':
      clause[flt.field] = {
        like : '*' + flt.value
      };
      break;
    default:
      throw new TypeError('Filter operator ' + flt.operator
        + ' is not supported.');
  }
  return clause;
}

function convertKendoFilter(filter) {
  var restFilter = {};
  // TODO: fix filter 'eq' operator bug
  if (typeof (filter) === 'string') {
    filter = JSON.parse(filter);
  }
  if (filter.filters) {
    if (filter.filters.length === 1) {
      restFilter = convertKendoFilter(filter.filters[0]);
      return restFilter;
    }
    restFilter[filter.logic] = [];

    filter.filters.forEach(function(flt) {
      restFilter[filter.logic].push(convertKendoFilter(flt));
    });

  } else {
    return getClauseFromKendo(filter);
  }

  return restFilter;
}
module.exports = JSDOHandler;
