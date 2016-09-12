var rsvp = require('rsvp');
var akera = require('akera-api');
var f = akera.query.filter;

function AkeraCrud() {}

var addSort = function(qry, field, desc) {
  if (typeof field === 'string') {
    qry.by(field, desc || false);
  }
  if (typeof field === 'object') {
    if (field instanceof Array) {
      for ( var fld in field) {
        addSort(qry, field[fld]);
      }
    } else {
      var keys = Object.keys(field);
      if (keys.length === 1)
        addSort(qry, keys[0], field[keys[0]]);
    }
  }
};

var getWhereFilter = function(table, filter) {
  if (typeof filter === 'object') {

    if (filter.rowid) {
      return f.rowid(table, filter.rowid);
    } else {
      var filters = [];

      if (typeof filter.pk === 'object') {
        for ( var key in filter.pk) {
          filters.push(f.eq(key, filter.pk[key]));
        }
      }

      if (typeof filter.where === 'object')
        filters.push(filter.where);

      if (filters.length > 0)
        return filters.length === 1 ? filters[0] : f.and(filters);

    }
  }

  return null;
};

var transformData = function(data, sqlMap, back) {
  if (data && sqlMap && sqlMap.length) {
    if (data instanceof Array) {
      data.forEach(function(row) {
        transformData(row, sqlMap, back);
      });
    } else {
      sqlMap.forEach(function(field) {
        if (field.alias !== undefined) {
          if (back === true) {
            if (data[field.name] !== undefined) {
              data[field.alias] = data[field.name];
              delete data[field.name];
            }
          } else {
            if (data[field.alias] !== undefined) {
              data[field.name] = data[field.alias];
              delete data[field.alias];
            }
          }
        }
      });
    }
  }

  return data;
};

var transformWhere = function(where, table, sqlMap) {
  // rowid value
  if (typeof where === 'string') {
    where = f.rowid(table, where);
  } else {
    where = transformData(where, sqlMap);

    switch (Object.keys(where).length) {
      case 0:
        return null;
      case 1:
        return where;
      default:
        var filters = [];

        for ( var key in where) {
          filters.push(f.eq(key, where[key]));
        }

        where = f.and(filters);
    }
  }

  return where;
};

AkeraCrud.prototype.create = function(broker, table, data, sqlMap) {
  var self = this;

  return new rsvp.Promise(function(resolve, reject) {
    var _conn = null;

    akera.connect(broker).then(function(conn) {
      _conn = conn;

      return conn.query.insert(table).set(transformData(data, sqlMap)).fetch();
    }).then(function(result) {
      self.disconnect(_conn, resolve, transformData(result, sqlMap, true));
    })['catch'](function(err) {
      self.disconnect(_conn, reject, err);
    });
  });
};

AkeraCrud.prototype.read = function(broker, table, filter) {
  var self = this;

  return new rsvp.Promise(function(resolve, reject) {
    var _conn = null;

    akera.connect(broker).then(function(conn) {
      _conn = conn;

      var qry = conn.query.select(table);

      if (typeof filter === 'string') {
        try {
          filter = JSON.parse(filter);
        } catch (err) {
        }
      }
      
      if (typeof filter === 'object') {

        filter.offset = filter.offset || filter.start || filter.skip;
        filter.top = filter.top || filter.limit;
        filter.fields = filter.fields || filter.select;

        var where = getWhereFilter(table, filter);

        if (where)
          qry.where(where);

        if (filter.count === true)
          return qry.count();
        
        if (filter.sort)
          addSort(qry, filter.sort);

        if (filter.offset)
          qry.offset(filter.offset);
        
        if (filter.top)
          qry.limit(filter.top);
        
        if (filter.fields)
          qry.fields(filter.fields);

      }

      return qry.all();
    }).then(function(result) {
      self.disconnect(_conn, resolve, result);
    })['catch'](function(err) {
      self.disconnect(_conn, reject, err);
    });
  });
};

AkeraCrud.prototype.update = function(broker, table, where, data, sqlMap) {
  var self = this;

  return new rsvp.Promise(function(resolve, reject) {
    var _conn = null;

    where = transformWhere(where, table, sqlMap);

    if (!where)
      return reject(new Error('Primary key value required for update.'));

    akera.connect(broker).then(
      function(conn) {
        _conn = conn;

        return conn.query.update(table).where(where).set(
          transformData(data, sqlMap)).fetch();
      }).then(function(result) {
      if (result.length === 0) {
        self.disconnect(_conn, reject, new Error('Record not found.'));
      } else {
        self.disconnect(_conn, resolve, transformData(result, sqlMap, true));
      }
    })['catch'](function(err) {
      self.disconnect(_conn, reject, err);
    });
  });
};

AkeraCrud.prototype.destroy = function(broker, table, where, sqlMap) {
  var self = this;

  return new rsvp.Promise(function(resolve, reject) {
    var _conn = null;

    where = transformWhere(where, table, sqlMap);

    if (!where)
      return reject(new Error('Primary key value required for delete.'));

    akera.connect(broker).then(function(conn) {
      _conn = conn;

      return conn.query.destroy(table).where(where).go();
    }).then(function(result) {
      self.disconnect(_conn, resolve, result);
    })['catch'](function(err) {
      self.disconnect(_conn, reject, err);
    });
  });
};

AkeraCrud.prototype.disconnect = function(conn, cb, data) {
  if (!conn)
    return cb && cb(data);
  conn.disconnect().then(function() {
    return cb && cb(data);
  }, function() {
    return cb && cb(data);
  });
};

module.exports = AkeraCrud;
