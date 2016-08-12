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

describe('JSDO Metadata', function() {

  it('should load database services', function(done) {
    this.timeout(10000);

    jsdoMeta.getCatalog(null, null, true, broker).then(function(dbs) {
      should(dbs).be.an.instanceOf(Object);
      should(dbs).have.properties([ 'services' ]);
      should(dbs.services[0]).have.properties['resources'];
      dbs.services[0].resources.forEach(function(resource) {
        should(resource).have.properties['name', 'path', 'displayName', 'schema'];
        should(resource.schema).have.properties['type', 'properties'];
        Object.keys(resource.schema).forEach(function(ds) {
          should(ds.indexOf('ds')).equal(0);
          Object.keys(resource.schema.properties).forEach(function(tt) {
            should(tt.indexOf('tt')).equal(0);
          });
        });
      });
      console.log(dbs);
      done();
    })['catch'](function(err) {
      console.log(err);
      done(err);
    });

  });

});
