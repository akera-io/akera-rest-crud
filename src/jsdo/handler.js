var JSDOCatalog = require('./metadata.js');
var metadata = new JSDOCatalog();

function JSDOHandler(restHandler) {

  var self = this;

  this.restHandler = restHandler;

  this.asDataset = function(asDataset) {
    this.asDataset = asDataset || false;
  };

  this.getCatalog = function(req, res) {
    var tableName = req.params.table;
    var dbName = req.params.db;

    metadata.getCatalog(dbName, tableName, self.asDataset === true, req.broker,
      res);
  };

  this.doSelect = function(req, res) {
    if ((!req.query.jsdoFilter || req.query.jsdoFilter === '')
      && (!req.query.top || req.query.top === '')
      && (!req.query.skip || req.query.skip === '')
      && (!req.query.sort || req.query.sort === ''))
    {
      sanitizeQueryString(req);
      return self.restHandler.doSelect(req, res);
    }

    var filter = {};
    if (req.query.jsdoFilter && req.query.jsdoFilter !== '') {
      filter = self.filter.fromKendo(req.query.jsdoFilter);
    }
    if (req.query.top && req.query.top !== '') {
      filter.limit = parseInt(req.query.top);
    }
    if (req.query.skip && req.query.skip !== '') {
      filter.offset = parseInt(req.query.skip);
    }

    if (req.query.sort) {
      if (typeof (req.query.sort) === 'string') {
        try {
          req.query.sort = JSON.parse(req.query.sort);
        } catch (e) {}
      }
      if (req.query.sort instanceof Array) {
        if (req.query.sort.length > 0) {
          filter.by = {
            field : req.query.sort[0].field,
            descending : req.query.sort[0].dir !== 'asc'
          };
        }
      } else if (req.query.sort !== '') {
        filter.by = {
          field : req.query.sort.field,
          descending : req.query.sort.dir !== 'asc'
        };
      }
    }
    req.query.filter = filter;

    sanitizeQueryString(req);

    self.restHandler._select(req, function(err, rows) {
      if (err) {
        return self.restHandler.error(err);
      }
      if (self.asDataset === true) {
        var ds = {};
        ds['ds' + req.params.table] = {

        };
        ds['ds' + req.params.table]['tt' + req.params.table] = rows;

        return res.status(200).json(ds);
      }
      res.status(200).json(rows);
    });
  };

  this.doCreate = function(req, res) {
    if (req.body) {
      delete req.body._id;
      req.body = self.asDataset === true ? getDataFromDataset(req) : req.body;
      self.restHandler._create(req, function(err, row) {
        if (err) {
          return self.restHandler.error(err, res);
        }
        return res.status(200)
          .json(
            self.asDataset === true ? formatResponseAsDataset(req, row)
              : [ row ]);
      });
    } else {
      res.status(400).end();
    }
  };

  this.doUpdate = function(req, res) {
    if (req.body) {
      delete req.body._id;
    }
    self.restHandler.doUpdate(req, res);
  };

  this.doCount = function(req, res) {
    if (!req.query.filter || req.query.filter === '') {
      return self.restHandler.doCount(req, res);
    }
    req.query.filter = self.filter.fromKendo(req.query.filter);
    self.restHandler.doCount(req, res);
  };

  this.filter = {
    fromKendo : function(kendoFilter) {
      return {
        where : convertKendoFilter(kendoFilter)
      };
    }
  };

}

function getDataFromDataset(req) {
  var tts = req.body['tt' + req.params.table];
  if (tts instanceof Array) {
    tts = tts[0];
  }
  delete tts._id;
  return tts;
}
function formatResponseAsDataset(req, row) {
  var ttRet = {};
  ttRet['ds' + req.params.table] = {};
  ttRet['ds' + req.params.table]['tt' + req.params.table] = [ row ];
  return ttRet;
}
function sanitizeQueryString(req) {
  delete req.query.jsdoFilter;
  delete req.query.top;
  delete req.query.skip;
  delete req.query.sort;
}

function getClauseFromKendo(flt) {
  var clause = {};
  switch (flt.operator) {
    case 'eq':
      clause[flt.field] = {
        eq : flt.value
      };
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
        matches : '*' + flt.value + '*'
      };
      break;
    case 'doesnotcontain': {
      clause[flt.field] = {
        not : {
          matches : '*' + flt.value + '*'
        }
      };
      break;
    }
    case 'startswith':
      clause[flt.field] = {
        matches : flt.value + '*'
      };
      break;
    case 'endswith':
      clause[flt.field] = {
        matches : '*' + flt.value
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
