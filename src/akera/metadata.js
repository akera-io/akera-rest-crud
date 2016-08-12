var akera = require('akera-api');
var async = require('async');
var rsvp = require('rsvp');

function AkeraMetaData() {
  this.cache = {};

}

AkeraMetaData.prototype.getDatabases = function(broker, fullLoad) {
  var self = this;

  return new rsvp.Promise(function(resolve, reject) {
    var brokerInfo = self.cache[broker.alias];

    if (brokerInfo && (fullLoad !== true || brokerInfo.fullyLoaded === true)) {
      return resolve(brokerInfo.db);
    }

    akera.connect(broker).then(function(conn) {
      return conn.getMetaData().allDatabases();
    }).then(function(dbs) {
      brokerInfo = brokerInfo || {
        fullyLoaded : fullLoad === true
      };
      brokerInfo.db = brokerInfo.db || {};

      if (fullLoad === true) {
        async.forEach(dbs, function(db, cb) {
          getDatabaseService(db, null, asDataset, function(err, service) {
            root.services.push(service);
            cacheMngr.storeService(service);
            ccb(err);
          });
        }, function(err) {
          if (err)
            return reject(err);

        });

      } else {
        dbs.forEach(function(db) {
          var dbName = db.getName();

          if (!brokerInfo.db[dbName])
            brokerInfo.db[dbName] = {};
        });
        self.cache[broker.alias] = brokerInfo;
        resolve(brokerInfo.db);
      }
    })['catch'](reject);
  });
};

AkeraMetaData.prototype.getTables = function(broker, db, fullLoad) {

};

AkeraMetaData.prototype.getTable = function(broker, db, table) {

};

module.exports = AkeraMetaData;
