var should = require('should');
var AkeraHandler = require('../src/akera/handler.js');
var akera = new AkeraHandler();
var akeraMeta = akera.getMetaData();
var JSDOMetadata = require('../src/jsdo/metadata.js');
var jsdoMeta = new JSDOMetadata(akeraMeta);

var broker = {
  alias : 'sports',
  host : '10.10.10.6',
  port : 38900
};

var badBroker = {
  alias : 'sportsbad',
  host : '10.10.10.6',
  port : 33333
};

var checkTable = function(tt) {
  should(tt).have.properties['type', 'items'];

  Object.keys(tt.items.properties).forEach(function(field) {
    should(tt.items.properties[field]).have.property('type');
  });
};

var checkDataset = function(ds) {
  should(ds).have.properties['name', 'path', 'displayName', 'properties'];

  Object.keys(ds.properties).forEach(function(tt) {
    checkTable(ds.properties[tt]);
  });
};

var checkResource = function(resource) {
  should(resource).have.properties['name', 'path', 'displayName', 'schema'];

  Object.keys(resource.schema.properties).forEach(function(ds) {
    checkDataset(resource.schema.properties[ds]);
  });
};

describe('JSDO Metadata (as datasets)', function() {

  it('should fail to load all services with invalid broker', function(done) {
    this.timeout(10000);

    jsdoMeta.getCatalog(null, null, true, badBroker).then(function(info) {
      done(new Error('Should have failed but returned: ' + info));
    }, function(err) {
      done();
    });

  });

  it('should load all database services', function(done) {
    this.timeout(10000);

    jsdoMeta.getCatalog(null, null, true, broker).then(
      function(dbs) {
        try {
          should(dbs).be.an.instanceOf(Object);
          should(dbs).have.properties([ 'services' ]);
          should(dbs.services[0]).have.properties['resources'];
          dbs.services[0].resources.forEach(function(resource) {
            checkResource(resource);
          });

          done();
        } catch (err) {
          done(err);
        }
      }, done);

  });

  it('should fail to load sports2000 service with invalid broker', function(
    done)
  {
    this.timeout(10000);

    jsdoMeta.getCatalog('sports2000', null, true, badBroker).then(
      function(info) {
        done(new Error('Should have failed but returned: ' + info));
      }, function(err) {
        done();
      });

  });

  it('should load sports2000 service', function(done) {
    this.timeout(10000);

    jsdoMeta.getCatalog('sports2000', null, true, broker).then(
      function(dbs) {
        try {
          should(dbs).be.an.instanceOf(Object);
          should(dbs).have.properties([ 'services' ]);
          should(dbs.services[0]).have.properties['resources'];
          dbs.services[0].resources.forEach(function(resource) {
            checkResource(resource);
          });

          done();
        } catch (err) {
          done(err);
        }
      }, done);

  });

  it('should fail to load customer service with invalid broker', function(done)
  {
    this.timeout(10000);

    jsdoMeta.getCatalog('sports2000', 'customer', true, badBroker).then(
      function(info) {
        done(new Error('Should have failed but returned: ' + info));
      }, function(err) {
        done();
      });

  });

  it('should load only customer service', function(done) {
    this.timeout(10000);

    jsdoMeta.getCatalog('sports2000', 'customer', true, broker).then(
      function(dbs) {
        try {
          should(dbs).be.an.instanceOf(Object);
          should(dbs).have.properties([ 'services' ]);
          should(dbs.services[0]).have.properties['resources'];
          dbs.services[0].resources.forEach(function(resource) {
            checkResource(resource);
          });

          done();
        } catch (err) {
          done(err);
        }
      }, done);

  });
});
