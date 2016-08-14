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

AkeraCrud.prototype.create = function(broker, table, data) {
  var self = this;

  return new rsvp.Promise(function(resolve, reject) {
    var _conn = null;

    akera.connect(broker).then(function(conn) {
      _conn = conn;
      return conn.query.insert(table).set(data).fetch();
    }).then(function(result) {
      self.disconnect(_conn, resolve, result);
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

      if (typeof filter === 'object') {
        var filters = [];

        if (typeof filter.pk === 'object') {
          for ( var key in filter.pk) {
            filters.push(f.eq(key, filter.pk[key]));
          }
        }

        if (typeof filter.where === 'object')
          filters.push(filter.where);

        if (filters.length > 0)
          qry.where(filters.length === 1 ? filters[0] : f.and(filters));

        if (filter.sort)
          addSort(qry, filter.sort);

        filter.offset = filter.offset || filter.start;
        filter.top = filter.top || filter.limit;

        if (filter.offset)
          qry.offset(filter.offset);
        if (filter.top)
          qry.limit(filter.top);
      }

      return qry.all();
    }).then(function(result) {
      self.disconnect(_conn, resolve, result);
    })['catch'](function(err) {
      self.disconnect(_conn, reject, err);
    });
  });
};

AkeraCrud.prototype.update = function(broker, table, pk, data) {
  var self = this;

  return new rsvp.Promise(function(resolve, reject) {
    var _conn = null;

    if (typeof pk !== 'object' || Object.keys(pk).length === 0)
      return reject(new Error('Primary key value required for update.'));

    akera.connect(broker).then(function(conn) {
      _conn = conn;

      var filters = [];

      for ( var key in pk) {
        filters.push(f.eq(key, pk[key]));
      }

      var where = filters.length === 1 ? filters[0] : f.and(filters);

      return conn.query.update(table).where(where).set(data).fetch();
    }).then(function(result) {
      if (result.length === 0)
        self.disconnect(_conn, reject, new Error('Record not found.'));
      else
        self.disconnect(_conn, resolve, result);
    })['catch'](function(err) {
      self.disconnect(_conn, reject, err);
    });
  });
};

AkeraCrud.prototype.destroy = function(broker, table, pk) {
  var self = this;

  return new rsvp.Promise(function(resolve, reject) {
    var _conn = null;

    if (typeof pk !== 'object' || Object.keys(pk).length === 0)
      return reject(new Error('Primary key value required for delete.'));

    akera.connect(broker).then(function(conn) {
      _conn = conn;

      var filters = [];

      for ( var key in pk) {
        filters.push(f.eq(key, pk[key]));
      }

      var where = filters.length === 1 ? filters[0] : f.and(filters);

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
