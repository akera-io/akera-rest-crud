var akera = require('akera-api');
var f = akera.query.filter;

var operatorMap = {
  neq: 'ne',
  gte: 'ge',
  lte: 'le',
  contains: 'like'
};

module.exports = {
  fromKendo: function(kendoFilter) {
    return recursiveConvert(kendoFilter);
  }
};

function recursiveConvert(filter) {
  //TODO: fix filter 'eq' operator bug
  if (typeof(filter) === 'string') {
    filter = JSON.parse(filter);
  }
  if (filter.filters) {
    var conditions = [];
    filter.filters.forEach(function(flt) {
      conditions.push(recursiveConvert(flt));
    });
    return f[filter.logic](conditions);
  } else {
    filter.operator = operatorMap[filter.operator] || filter.operator;
    //TODO: implement a getClause function to handle 'contains', 'startsWith', etc. operators
    return f[filter.operator](filter.field, filter.operator === 'like' ? '*' + filter.value + '*' : filter.value);
  }
}