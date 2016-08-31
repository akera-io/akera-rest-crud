var akera = require('akera-api');
var f = akera.query.filter;

var rollbaseNoSpaceOperators = [ '=', '>', '<', '<>', '<=', '>=' ];

function FilterParser() {}

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

var convertRollbaseCriteria = function(flt) {

  if (!flt || !flt.operator)
    return null;

  switch (flt.operator) {
    case '=':
      flt.operator = f.operator.eq;
      break;
    case '<>':
      flt.operator = f.operator.ne;
      break;
    case '>':
      flt.operator = f.operator.gt;
      break;
    case '<':
      flt.operator = f.operator.lt;
      break;
    case '>=':
      flt.operator = f.operator.ge;
      break;
    case '<=':
      flt.operator = f.operator.le;
      break;
  }

  return f[flt.operator](flt.field, flt.value);
}

var parseGroup = function(filter, tree) {

  var hasPhar = filter.charAt(0) === '(';
  var group = {
    tokens : []
  };

  if (hasPhar)
    filter = filter.substr(1).trim();

  while (filter !== null && filter.length > 0) {

    if (hasPhar && filter.charAt(0) === ')')
      break;

    // conditions are grouped with logical operators
    if (group.tokens.length > 0) {
      var op = {};
      filter = parseOperator(filter, op);
      group.group = group.group || op.operator;

      if (group.group !== op.operator)
        throw new Error('All logical operators in a group must be the same.');
    }

    // we have a nested group
    if (filter.charAt(0) === '(')
      filter = parseGroup(filter, group);
    else {
      var criteria = {};
      filter = parseField(filter, criteria);
      filter = parseOperator(filter, criteria);
      filter = parseValue(filter, criteria);

      group.tokens.push(criteria);
    }
  }

  if (hasPhar) {
    if (!filter || filter.charAt(0) !== ')')
      throw new Error('Invalid group');
    filter = filter.substr(1);
  }

  tree.tokens = tree.tokens || [];
  tree.tokens.push(group);

  return filter ? filter.trim() : null;

}

var parseField = function(filter, node) {
  var idx = filter.indexOf(' ');
  var spLen = 1;

  for ( var op in rollbaseNoSpaceOperators) {
    var opIdx = filter.indexOf(rollbaseNoSpaceOperators[op]);
    if (opIdx !== -1 && (idx === -1 || opIdx < idx)) {
      idx = opIdx;
      spLen = 0;
      break;
    }
  }

  if (idx === -1) {
    node.field = filter;
    return null;
  }

  node.field = filter.substr(0, idx);
  return filter.substr(idx + spLen).trim();
}

var parseOperator = function(filter, node) {
  var idx = filter.indexOf(' ');

  for ( var op in rollbaseNoSpaceOperators) {
    var opIdx = filter.indexOf(rollbaseNoSpaceOperators[op]);
    if (opIdx !== -1 && (idx === -1 || opIdx < idx)) {
      idx = rollbaseNoSpaceOperators[op].length;
      break;
    }
  }

  if (idx === -1) {
    node.operator = filter.toLowerCase();
    return null;
  }

  node.operator = filter.substr(0, idx).toLowerCase();
  return filter.substr(idx).trim();
}

var parseValue = function(filter, node) {
  var value = null;
  var idx = 1;

  if (filter.charAt(0) === '\'') {
    while (idx < filter.length) {
      if (filter.charAt(idx) === '\'') {

        if (idx === filter.length - 1 || filter.charAt(idx + 1) !== '\'') {
          value = filter.substr(1, idx - 1);
          filter = idx === filter.length ? null : filter.substr(idx + 1).trim();
          break;
        }

        idx++;
      }

      idx++;
    }

    if (value === null)
      throw new Error('Invalid string value.');

  } else {
    idx = filter.indexOf(' ');

    if (idx === -1) {
      value = filter;
      filter = null;
    } else {
      value = filter.substr(0, idx);
      filter = filter.substr(idx + 1).trim();
    }

    if (value === '?')
      value = null;
    else if (!isNaN(value))
      value = parseFloat(value);
    else
      value = value === 'true';
  }

  node.value = value;

  return filter;
}

var convertRollbaseTree = function(tree) {

  if (!tree)
    return null;

  if (tree.group) {
    var conditions = tree.tokens.map(function(token) {
      return convertRollbaseTree(token);
    });

    return f[tree.group](conditions);
  } else {
    if (tree.field && tree.value !== undefined) {
      tree.operator = tree.operator || f.operator.eq;
      convertRollbaseCriteria(tree);
      return f[tree.operator](tree.field, tree.value);
    }

    if (tree.tokens && tree.tokens.length === 1)
      return convertRollbaseTree(tree.tokens[0]);
  }

  return null;
}

var fromRollbase = FilterParser.fromRollbase = function(filter) {
  var tree = {
    tokens : []
  };

  var ablFilter = null;

  try {
    ablFilter = JSON.parse(filter);

    if (ablFilter.ablFilter)
      filter = ablFilter;
    else
      return null;
  } catch (err) {}

  if (ablFilter) {
    filter = ablFilter;
  }

  while (filter !== null) {
    // conditions are grouped with logical operators
    if (tree.tokens.length > 0) {
      var op = {};
      filter = parseOperator(filter, op);
      tree.group = tree.group || op.operator;

      if (tree.group !== op.operator)
        throw new Error('All logical operators in a group must be the same.');
    }

    filter = parseGroup(filter, tree);
  }

  return convertRollbaseTree(tree);
}

FilterParser.convert = function(filter) {
  if (!filter || (typeof filter === 'string' && filter.trim().length === 0))
    return null;

  try {
    var kendoFilter = fromKendo(filter);
    if (kendoFilter) {
      return kendoFilter;
    }
  } catch (err) {}

  return fromRollbase(filter);
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

  var group = filter.logic || 'and';
  filter = filter.filters || filter;

  if (filter instanceof Array) {
    if (filter.length === 1) {
      return convertKendoCriteria(filter[0]);
    } else {
      return f[group](filter.map(function(flt) {
        return convertKendoCriteria(flt);
      }));
    }
  } else {
    return convertKendoCriteria(filter);
  }
}

module.exports = FilterParser;
