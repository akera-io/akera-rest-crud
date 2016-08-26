var akera = require('akera-api');
var f = akera.query.filter;

function FilterParser() {}

var decodeValue = function(value) {
  if (value === undefined || value === null || value === '?')
    return null;

  if (typeof value !== 'string')
    return value;

  value = value.trim();

  if (value.indexOf('\'') === 0)
    return value.substr(1, value.length - 2).replace(/''/g, '\'');

  if (!isNaN(value))
    return parseFloat(value);

  return value === 'true';
}

var convertKendoCriteria = function(flt) {

  if (!flt || !flt.operator)
    return null;

  switch (flt.operator) {
    case 'eq':
    case 'lt':
    case 'gt':
    case 'contains':
      break;
    case 'neq':
      flt.operator = f.operator.ne;
      break;
    case 'gte':
      flt.operator = f.operator.ge;
      break;
    case 'lte':
      flt.operator = f.operator.le;
      break;
    case 'startswith':
      flt.operator = f.operator.matches;
      flt.value = flt.value + '*';
      break;
    case 'endswith':
      flt.operator = f.operator.matches;
      flt.value = '*' + flt.value;
      break;
    default:
      throw new TypeError('Filter operator ' + flt.operator
        + ' is not supported.');
  }

  return f[flt.operator](flt.field, flt.value);
}

var convert = FilterParser.convert = function(filter) {
  if (!filter || (typeof filter === 'string' && filter.trim().length === 0))
    return null;

  try {
    var kendoFilter = fromKendo(filter);
    if (kendoFilter)
      return kendoFilter;
  } catch (err) {}

  try {
    return fromRollbase(filter);
  } catch (err) {}
}

var fromKendo = FilterParser.fromKendo = function(filter) {
  if (!filter)
    return null;

  if (typeof (filter) === 'string') {
    filter = filter.trim();

    if (filter.length === 0)
      return null;

    filter = JSON.parse(filter);
  }

  if (filter.filters)
    return fromKendo(filter.filters);

  if (filter.filters) {
    if (filter.filters.length === 1) {
      return convertKendoCriteria(filter.filters[0]);
    }

    var filters = filter.filters.map(function(flt) {
      return convertKendoCriteria(flt);
    });

    return f[filter.logic](filters);
  }

}

var fromRollbase = FilterParser.fromRollbase = function(filter, init) {
  if (!filter)
    return null;

  if (filter.ablFilter)
    return fromRollbase(filter.ablFilter);

  if (typeof filter !== 'string')
    throw new Error('Invalid filter');

  try {
    var jsonFilter = JSON.parse(filter);
    return fromRollbase(jsonFilter.ablFilter);
  } catch (err) {}

  var chunks = null;

  // special case for PK filter: key=value
  if (filter.indexOf('=') !== -1
    && (filter.indexOf(' ') === -1 || filter.indexOf('=') < filter.indexOf(' ')))
  {
    chunks = filter.split('=');
    return f.eq(chunks[0], decodeValue(chunks[1]));
  }

  init = init || {
    criteria : []
  };

  chunks = filter.split(' ');
  var field = null;
  var op = null;
  var val = null;
  var strVal = null;
  var lastIdx = 0;
  var group = null;

  for ( var i in chunks) {
    var entry = chunks[i];

    if (!field || !op) {
      if (entry === '')
        continue;

      if (!field)
        field = entry;
      else
        op = entry.toLowerCase();
      continue;
    } else {
      if (val === null) {
        if (strVal !== null) {
          strVal += ' ' + entry;
          if (entry !== '' && entry.lastIndexOf('\'') === entry.length - 1)
            val = strVal;
        } else {
          if (entry.indexOf('\'') === 0) {
            lastIdx = entry.lastIndexOf('\'');
            if (lastIdx > 0 && entry.lastIndexOf('\'', lastIdx) !== lastIdx - 1)
              val = entry;
            else
              strVal = entry;
          } else
            val = entry;
        }

        if (val !== null) {
          init.criteria.push(f[op](field, decodeValue(val)));
          filter = '';
        }
      } else {
        if (group === null) {
          if (entry !== '') {
            group = entry.toLowerCase();
            init.group = init.group || group;
          }
          continue;
        }

        filter += ' ' + entry;
      }
    }
  }

  filter = filter.trim();

  if (filter.length > 0)
    fromRollbase(filter, init);

  if (init.criteria.length === 1)
    return init.criteria[0];

  return f[init.group || 'and'](init.criteria);
}

module.exports = FilterParser;
