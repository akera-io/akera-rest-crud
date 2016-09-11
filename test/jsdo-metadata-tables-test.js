var should = require('should');
var AkeraHandler = require('../src/akera/handler.js');
var akera = new AkeraHandler();
var akeraMeta = akera.getMetaData();
var JSDOMetadata = require('../src/jsdo/metadata.js');
var jsdoMeta = new JSDOMetadata(akeraMeta, false, true);

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

describe('JSDO Metadata (as tables)', function() {

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
            should(resource).have.properties['name', 'path', 'displayName',
              'schema'];
            should(resource.schema).have.properties['type', 'properties'];
            Object.keys(resource.schema.properties).forEach(
              function(table) {
                should(resource.schema.properties[table]).have
                  .property('items');
                should(resource.schema.properties[table].items).be.an
                  .instanceOf(Object);
                should(resource.schema.properties[table].items).have
                  .property('properties');
              });
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
            should(resource).have.properties['name', 'path', 'displayName',
              'schema'];
            should(resource.schema).have.properties['type', 'properties'];
            Object.keys(resource.schema.properties).forEach(
              function(table) {
                Object.keys(resource.schema.properties).forEach(
                  function(table) {
                    should(resource.schema.properties[table]).have
                      .property('items');
                    should(resource.schema.properties[table].items).be.an
                      .instanceOf(Object);
                    should(resource.schema.properties[table].items).have
                      .property('properties');
                  });
              });
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
            should(resource).have.properties['name', 'path', 'displayName',
              'schema'];
            should(resource.schema).have.properties['type', 'properties'];
            Object.keys(resource.schema.properties).forEach(
              function(table) {
                Object.keys(resource.schema.properties).forEach(
                  function(table) {
                    should(resource.schema.properties[table]).have
                      .property('items');
                    should(resource.schema.properties[table].items).be.an
                      .instanceOf(Object);
                    should(resource.schema.properties[table].items).have
                      .property('properties');
                  });
              });
          });

          done();
        } catch (err) {
          done(err);
        }
      }, done);

  });
});
