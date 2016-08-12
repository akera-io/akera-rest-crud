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

  it('should load databases', function(done) {
    this.timeout(10000);

    meta.getDatabases(broker).then(function(dbs) {
      should(dbs).be.an.instanceOf(Object);
      should(dbs).have.properties([ 'sports2000' ]);
      done();
    })['catch'](function(err) {
      console.log(err);
      done(err);
    });

  })

});
