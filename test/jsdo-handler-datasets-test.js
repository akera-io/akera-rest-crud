var should = require('should');
var RootHandler = require('../src/akera-rest-crud.js');
var root = new RootHandler();
var JSDOHandler = require('../src/jsdo/handler.js');
var jsdo = new JSDOHandler(root);
var Response = require('./response.js');
var Request = require('./request.js');
var router = require('./router.js');
var config = {
  jsdo : {
    asDataset : true
  }
};
var newCustomer = null;

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

describe('JSDO Handler - Initialization (as datasets)', function() {
  it('should initialize correctly', function(done) {
    jsdo.init(config, router);
    done();
  });
});

describe('JSDO Handler - Metadata (as datasets)', function() {

  it('should fail to load all services with invalid broker', function(done) {
    this.timeout(10000);

    var req = new Request().reqParams({
      db : 'sports2000'
    }).reqBroker(badBroker);

    jsdo.getCatalog(req, new Response(function(err, data) {
      if (err)
        done();
      else
        done(new Error('Should have returned connection error'));
    }));
  });

  it('should load all services not given db name and table name',
    function(done) {
      this.timeout(20000);

      var req = new Request().reqBroker(broker).reqParams({});

      jsdo.getCatalog(req, new Response(function(err, data) {
        if (err)
          done(new Error(err.message));
        else {
          try {
            should(data.version).be.exactly('1.3');
            should(data).have.property('services');
            should(data.services instanceof Array).be['true']();
            should(data.services.length > 0).be['true']();
            var sportsService = data.services[0];
            should(sportsService.name).be.exactly('sports2000');
            should(sportsService).have.property('resources');
            should(sportsService.resources instanceof Array).be['true'];
            should(sportsService.resources.length > 0).be['true'];
            var benefitsResource = sportsService.resources[0];
            should(benefitsResource.name).be.exactly('benefits');
            should(benefitsResource.path).be.exactly('/benefits');
            should(benefitsResource.schema).have.property('properties');
            should(benefitsResource.schema.properties).have
              .property('dsbenefits');
            done();
          } catch (e) {
            done(new Error(e.message));
          }
        }
      }));
    });

  it('should load sports service with only customers resource', function(done) {
    this.timeout(20000);

    var req = new Request().reqParams({
      db : 'sports2000',
      table : 'customer'
    }).reqBroker(broker);

    jsdo.getCatalog(req, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          should(data.services.length).be.exactly(1);
          var sportsService = data.services[0];
          should(sportsService.name).be.exactly('sports2000');
          should(sportsService.resources.length).be.exactly(1);
          var customersResource = sportsService.resources[0];
          should(customersResource.name).be.exactly('customer');
          should(customersResource.schema.properties).have
            .property('dscustomer');
          done();
        } catch (e) {
          done(new Error(e.message));
        }
      }
    }));
  });

});

describe('JSDO Handler - Crud (as datasets)', function() {
  it('should fail to read with invalid broker', function(done) {
    this.timeout(20000);

    var req = new Request().reqBroker(badBroker).reqParams({
      db : 'sports2000',
      table : 'customer'
    });

    jsdo.doSelect(req, new Response(function(err, data) {
      if (err)
        done();
      else
        done(new Error('Should have returned connection error'));
    }));

  });

  it('should load customer records', function(done) {
    this.timeout(20000);

    var req = new Request().reqBroker(broker).reqParams({
      db : 'sports2000',
      table : 'customer'
    });

    jsdo.doSelect(req, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          should(data).have.property('dscustomer');
          should(data.dscustomer).have.property('ttcustomer');
          var ttCustomer = data.dscustomer.ttcustomer;
          should(ttCustomer).be.an.instanceOf(Array);
          should(ttCustomer[0]).have.properties([ 'CustNum', 'Name', 'Country',
            'City' ]);
          done();
        } catch (e) {
          done(new Error(e.message));
        }
      }
    }));
  });

  it('should read customer records with pagination', function(done) {
    this.timeout(10000);

    var req = new Request().reqBroker(broker).reqParams({
      db : 'sports2000',
      table : 'customer'
    }).queryString({
      top : 5,
      offset : 2
    });

    jsdo.doSelect(req, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          var ttCustomer = data.dscustomer.ttcustomer;
          should(ttCustomer).be.an.instanceOf(Array);
          (ttCustomer.length).should.be.exactly(5);
          should(ttCustomer[0]).have.properties([ 'CustNum', 'Name', 'Country',
            'City' ]);
          (ttCustomer[0].CustNum).should.be.exactly(1);
          done();
        } catch (err) {
          done(err);
        }

      }
    }));
  });

  it('should read customer records with filter', function(done) {
    this.timeout(10000);

    var req = new Request().reqParams({
      db : 'sports2000',
      table : 'customer'
    }).reqBroker(broker).queryString({
      filter : {
        logic : 'and',
        filters : [ {
          operator : 'eq',
          value : 5,
          field : 'CustNum'
        } ]
      }
    });
    jsdo.doSelect(req, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          var ttCustomer = data.dscustomer.ttcustomer;
          should(ttCustomer).be.an.instanceOf(Array);
          (ttCustomer.length).should.be.exactly(1);
          should(ttCustomer[0]).have.properties([ 'CustNum', 'Name', 'Country',
            'City' ]);
          (ttCustomer[0].CustNum).should.be.exactly(5);
          done();
        } catch (err) {
          done(err);
        }

      }
    }));
  });

  it('should count customers', function(done) {
    this.timeout(10000);

    var req = new Request().reqParams({
      db : 'sports2000',
      table : 'customer'
    }).reqBroker(broker);

    jsdo.doCount(req, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          should(data).have.property('count');
          should(data.count).be.above(1);
          done();
        } catch (err) {
          done(err);
        }
      }
    }));
  });

  it('should count customers with filter', function(done) {
    this.timeout(10000);
    var req = new Request().reqParams({
      db : 'sports2000',
      table : 'customer'
    }).queryString({
      filter : {
        logic : 'and',
        filters : [ {
          operator : 'lt',
          value : 5,
          field : 'CustNum'
        } ]
      }
    }).reqBroker(broker);

    jsdo.doCount(req, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          should(data).have.property('count');
          should(data.count).be.exactly(4);
          done();
        } catch (err) {
          done(err);
        }
      }
    }));
  });

  it('should fail to create if invalid table', function(done) {
    this.timeout(10000);

    var name = 'Medu - ' + new Date().toString();
    
    var req = new Request().reqBroker(broker).reqParams({
      db : 'sports2000',
      table : 'progress'
    }).reqBody({
      ttcustomer : [ {
        Name : name,
        City : 'Cluj'
      } ]
    });

    jsdo.doCreate(req, new Response(function(err, data) {
      if (err)
        done();
      else
        done(new Error('Should have failed for invalid table'));
    }));
  });

  it('should create customer record', function(done) {
    this.timeout(10000);

    var name = 'Medu - ' + new Date().toString();
    var req = new Request().reqBroker(broker).reqParams({
      db : 'sports2000',
      table : 'customer'
    }).reqBody({
      ttcustomer : [ {
        Name : name,
        City : 'Cluj'
      } ]
    });

    jsdo.doCreate(req, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          should(data.dscustomer.ttcustomer).be.an.instanceOf(Array);
          data = data.dscustomer.ttcustomer[0];
          should(data).be.an.instanceOf(Object);
          should(data).have
            .properties([ 'CustNum', 'Name', 'Country', 'City' ]);
          (data.Name).should.be.exactly(name);
          (data.City).should.be.exactly('Cluj');
          (data.CustNum).should.be.above(1);
          newCustomer = data.CustNum;
          done();
        } catch (err) {
          done(err);
        }
      }
    }));
  });

  it('should update customer record', function(done) {
    this.timeout(10000);

    var addr = 'Cluj - ' + new Date().toString();

    var req = new Request().reqBroker(broker).reqParams({
      db : 'sports2000',
      table : 'customer'
    }).reqBody({
      dscustomer : {
        'prods:before' : {
          ttcustomer : [ {
            CustNum : 5
          } ]
        },
        ttcustomer : [ {
          Address2 : addr,
          Balance : 8080
        } ]
      }
    });
    jsdo.doUpdate(req, new Response(
      function(err, data) {
        if (err)
          done(new Error(err.message));
        else {
          try {
            should(data.dscustomer.ttcustomer).be.an.instanceOf(Array);
            data = data.dscustomer.ttcustomer;

            should(data[0]).have
              .properties([ 'CustNum', 'Balance', 'Address2' ]);
            (data[0].Address2).should.be.exactly(addr);
            (data[0].Balance).should.be.exactly(8080);
            done();
          } catch (err) {
            done(err);
          }
        }
      }));
  });

  it('should delete customer record', function(done) {
    this.timeout(10000);

    var req = new Request().reqBroker(broker).reqParams({
      db : 'sports2000',
      table : 'customer'
    }).reqBody({
      dscustomer : {
        'prods:before' : {
          ttcustomer : [ {
            CustNum : newCustomer
          } ]
        }
      }
    });
    jsdo.doDelete(req, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          (data).should.be.exactly(1);
          done();
        } catch (err) {
          done(err);
        }
      }
    }));
  });

});
