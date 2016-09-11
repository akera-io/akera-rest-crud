var should = require('should');
var AkeraHandler = require('../src/akera/handler.js');
var akera = new AkeraHandler();
var akeraMeta = akera.getMetaData();
var JSDOMetadata = require('../src/jsdo/metadata.js');
var jsdoMeta = new JSDOMetadata(akeraMeta, true, true);

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

var checkTable = function(tt, fields) {
  should(tt).have.properties['type', 'items'];

  var numFields = fields && fields.length ? fields.length : 0;
  
  Object.keys(tt.items.properties).forEach(function(field) {
    if (fields && fields.indexOf(field) !== -1)
      numFields--;
    
    should(tt.items.properties[field]).have.property('type');
  });
  
  if (fields)
    (numFields).should.be.exactly(0);
};

var checkDataset = function(ds, table, fields) {
  should(ds).have.properties['name', 'path', 'displayName', 'properties'];

  var tableFound = false;
  
  Object.keys(ds.properties).forEach(function(tt) {
    tableFound = tableFound || table === tt;
    checkTable(ds.properties[tt], fields);
  });
  
  if (fields)
    (tableFound).should.be.exactly(true);
};

var checkResource = function(resource, table, fields) {
  should(resource).have.properties['name', 'path', 'displayName', 'schema'];

  Object.keys(resource.schema.properties).forEach(function(ds) {
    checkDataset(resource.schema.properties[ds], table, fields);
  });
};

describe('JSDO Metadata (as datasets)', function() {

  it('should fail to load all services with invalid broker', function(done) {
    this.timeout(10000);

    jsdoMeta.getCatalog(null, null, badBroker).then(function(info) {
      done(new Error('Should have failed but returned: ' + info));
    }, function(err) {
      done();
    });

  });

  it('should load all database services', function(done) {
    this.timeout(10000);

    jsdoMeta.getCatalog(null, null, broker).then(
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

    jsdoMeta.getCatalog('sports2000', null, badBroker).then(
      function(info) {
        done(new Error('Should have failed but returned: ' + info));
      }, function(err) {
        done();
      });

  });

  it('should load sports2000 service', function(done) {
    this.timeout(10000);

    jsdoMeta.getCatalog('sports2000', null, broker).then(
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

    jsdoMeta.getCatalog('sports2000', 'customer', badBroker).then(
      function(info) {
        done(new Error('Should have failed but returned: ' + info));
      }, function(err) {
        done();
      });

  });

  it('should load only customer service', function(done) {
    this.timeout(10000);

    jsdoMeta.getCatalog('sports2000', 'customer', broker).then(
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
  
  it('should load only customer service', function(done) {
    this.timeout(10000);

    jsdoMeta.getCatalog('sports', 'customer', broker).then(
      function(dbs) {
        try {
          should(dbs).be.an.instanceOf(Object);
          should(dbs).have.properties([ 'services' ]);
          should(dbs.services[0]).have.properties['resources'];
          dbs.services[0].resources.forEach(function(resource) {
            checkResource(resource, 'customer', ['CustNum', 'City', 'Address']);
          });

          done();
        } catch (err) {
          done(err);
        }
      }, done);

  });
});
