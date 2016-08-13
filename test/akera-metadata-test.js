var should = require('should');
var AkeraHandler = require('../src/akera/handler.js');
var akera = new AkeraHandler();
var meta = akera.getMetaData();
var broker = {
  alias : 'sports',
  host : '10.10.10.6',
  port : 38900
};

describe('Akera Metadata', function() {

  it('should load table', function(done) {
    this.timeout(10000);

    meta.getTable(broker, 'sports2000', 'Customer').then(
      function(info) {
        try {
          should(info).be.an.instanceOf(Object);
          should(info.fields).have.properties([ 'CustNum', 'Name', 'City' ]);
          should(info.indexes).have
            .properties([ 'CustNum', 'Name', 'SalesRep' ]);
          done();
        } catch (err) {
          done(err);
        }
      }, done);

  });

  it('should load sport database', function(done) {
    this.timeout(10000);

    meta.getDatabase(broker, 'sports2000').then(function(db) {
      try {
        should(db).be.an.instanceOf(Object);
        should(db.table).have.properties([ 'Customer', 'Order', 'Item' ]);
        done();
      } catch (err) {
        done(err);
      }
    }, done);

  });

  it('should load sport database from cache', function(done) {
    meta.getDatabase(broker, 'sports2000').then(function(db) {
      try {
        should(db).be.an.instanceOf(Object);
        should(db.table).have.properties([ 'Customer', 'Order', 'Item' ]);
        done();
      } catch (err) {
        done(err);
      }
    }, done);

  });

  it('should fully load sport database', function(done) {
    this.timeout(10000);

    meta.getDatabase(broker, 'sports2000', true).then(
      function(db) {
        try {
          should(db).be.an.instanceOf(Object);
          should(db.table).have.properties([ 'Customer', 'Order', 'Item' ]);
          should(db.table.Customer.fields).have.properties([ 'CustNum', 'Name',
            'City' ]);
          should(db.table.Customer.indexes).have.properties([ 'CustNum',
            'Name', 'SalesRep' ]);
          done();
        } catch (err) {
          done(err);
        }
      }, done);

  });

  it('should load databases', function(done) {
    this.timeout(10000);

    meta.getDatabases(broker).then(function(dbs) {
      try {
        should(dbs).be.an.instanceOf(Object);
        should(dbs).have.properties([ 'sports2000' ]);
        done();
      } catch (err) {
        done(err);
      }
    }, done);

  });

  it('should fully load databases', function(done) {
    this.timeout(10000);

    meta.getDatabases(broker, true).then(
      function(dbs) {
        try {
          should(dbs).be.an.instanceOf(Object);
          should(dbs).have.properties([ 'sports2000' ]);
          should(dbs.sports2000.table).have.properties([ 'Customer', 'Order',
            'Item' ]);
          should(dbs.sports2000.table.Customer.fields).have.properties([
            'CustNum', 'Name', 'City' ]);
          should(dbs.sports2000.table.Customer.indexes).have.properties([
            'CustNum', 'Name', 'SalesRep' ]);
          done();
        } catch (err) {
          done(err);
        }
      }, done);

  });

  it('should load table from cache', function(done) {
    this.timeout(10000);

    meta.getTable(broker, 'sports2000', 'Customer').then(
      function(info) {
        try {
          should(info).be.an.instanceOf(Object);
          should(info.fields).have.properties([ 'CustNum', 'Name', 'City' ]);
          should(info.indexes).have
            .properties([ 'CustNum', 'Name', 'SalesRep' ]);
          done();
        } catch (err) {
          done(err);
        }
      }, done);

  });

});
