var should = require('should');
var RootHandler = require('../src/akera-rest-crud.js');
var root = new RootHandler();

var AkeraHandler = require('../src/akera/handler.js');
var akera = new AkeraHandler(root);
var Response = require('./response.js');
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

describe('Akera Handler - Metadata', function() {

  it('should fail to load all services with invalid broker', function(done) {
    this.timeout(10000);

    akera.getDatabases({
      broker : badBroker
    }, new Response(function(err, data) {
      if (err)
        done();
      else
        done(new Error('Should have returned connection error'));
    }));

  });

  it('should load databases', function(done) {
    this.timeout(10000);

    akera.getDatabases({
      broker : broker
    }, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          (data.length).should.be.above(1);
          (data[0]).should.startWith('sports');

          done();
        } catch (err) {
          done(err);
        }

      }
    }));

  });

  it('should load sports tables by index', function(done) {
    this.timeout(10000);

    akera.getTables({
      broker : broker,
      params : {
        db : 0
      }
    }, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          (data.length).should.be.exactly(25);
          (data[0]).should.be.exactly('benefits');
          (data[1]).should.be.exactly('billto');

          done();
        } catch (err) {
          done(err);
        }

      }
    }));

  });

  it('should load sports tables by name', function(done) {
    this.timeout(10000);

    akera.getTables({
      broker : broker,
      params : {
        db : 'sports2000'
      }
    }, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          (data.length).should.be.exactly(25);
          (data[0]).should.be.exactly('benefits');
          (data[1]).should.be.exactly('billto');

          done();
        } catch (err) {
          done(err);
        }

      }
    }));

  });

  it('should load customer fields by index', function(done) {
    this.timeout(10000);

    akera.getFields({
      broker : broker,
      params : {
        db : 0,
        table : 3
      }
    }, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          should(data).be.an.instanceOf(Object);
          should(data).have.properties([ 'CustNum', 'Name', 'City' ]);

          done();
        } catch (err) {
          done(err);
        }

      }
    }));

  });

  it('should load customer fields by name', function(done) {
    this.timeout(10000);

    akera.getFields({
      broker : broker,
      params : {
        db : 'sports2000',
        table : 'customer'
      }
    }, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          should(data).be.an.instanceOf(Object);
          should(data).have.properties([ 'CustNum', 'Name', 'City' ]);

          done();
        } catch (err) {
          done(err);
        }

      }
    }));

  });

  it('should load customer indexes by index', function(done) {
    this.timeout(10000);

    akera.getIndexes({
      broker : broker,
      params : {
        db : 0,
        table : 3
      }
    }, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          should(data).be.an.instanceOf(Object);
          should(data).have.properties([ 'CustNum', 'Name', 'CountryPost' ]);

          done();
        } catch (err) {
          done(err);
        }

      }
    }));

  });

  it('should load customer indexes by name', function(done) {
    this.timeout(10000);

    akera.getIndexes({
      broker : broker,
      params : {
        db : 'sports2000',
        table : 'customer'
      }
    }, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          should(data).be.an.instanceOf(Object);
          should(data).have.properties([ 'CustNum', 'Name', 'CountryPost' ]);

          done();
        } catch (err) {
          done(err);
        }

      }
    }));

  });

});

describe('Akera Handler - Crud', function() {

  it('should fail to read with invalid broker', function(done) {
    this.timeout(10000);

    akera.doSelect({
      broker : badBroker,
      params : {
        db : 'sports2000',
        table : 'customer'
      }
    }, new Response(function(err, data) {
      if (err)
        done();
      else
        done(new Error('Should have returned connection error'));
    }));

  });

  it('should read customer records', function(done) {
    this.timeout(10000);

    akera.doSelect({
      broker : broker,
      params : {
        db : 'sports2000',
        table : 'customer'
      }
    }, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {

          should(data).be.an.instanceOf(Array);
          should(data[0]).have.properties([ 'CustNum', 'Name', 'Country',
            'City' ]);

          done();
        } catch (err) {
          done(err);
        }

      }
    }));
  });

  it('should read customer records with pagination', function(done) {
    this.timeout(10000);

    akera.doSelect({
      broker : broker,
      params : {
        db : 'sports2000',
        table : 'customer'
      },
      query : {
        filter : {
          top : 5,
          offset : 2
        }
      }
    }, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {

          should(data).be.an.instanceOf(Array);
          (data.length).should.be.exactly(5);
          should(data[0]).have.properties([ 'CustNum', 'Name', 'Country',
            'City' ]);
          (data[0].CustNum).should.be.exactly(2);
          done();
        } catch (err) {
          done(err);
        }

      }
    }));
  });

  it('should read customer records with filter', function(done) {
    this.timeout(10000);

    akera.doSelect({
      broker : broker,
      params : {
        db : 'sports2000',
        table : 'customer'
      },
      query : {
        filter : {
          where : {
            CustNum : 5
          }
        }
      }
    }, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {

          should(data).be.an.instanceOf(Array);
          (data.length).should.be.exactly(1);
          should(data[0]).have.properties([ 'CustNum', 'Name', 'Country',
            'City' ]);
          (data[0].CustNum).should.be.exactly(5);
          done();
        } catch (err) {
          done(err);
        }

      }
    }));
  });

  it('should read customer record with pk', function(done) {
    this.timeout(10000);

    akera.doSelectRecord({
      broker : broker,
      params : {
        db : 'sports2000',
        table : 'customer',
        0 : '1'
      }
    }, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          should(data).be.an.instanceOf(Array);
          (data.length).should.be.exactly(1);
          should(data[0]).have.properties([ 'CustNum', 'Name', 'Country',
            'City' ]);
          (data[0].CustNum).should.be.exactly(1);
          done();
        } catch (err) {
          done(err);
        }

      }
    }));
  });

  it('should fail to read customer record if invalid pk values', function(done)
  {
    this.timeout(10000);

    akera.doSelectRecord({
      broker : broker,
      params : {
        db : 'sports2000',
        table : 'customer'
      }
    }, new Response(function(err, data) {
      if (err)
        done();
      else
        done(new Error('Read by pk whould have failed if invalid values.'))
    }));
  });

  it('should read orderline record with pk', function(done) {
    this.timeout(10000);

    akera.doSelectRecord({
      broker : broker,
      params : {
        db : 'sports2000',
        table : 'orderline',
        0 : '10/1'
      }
    }, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          should(data).be.an.instanceOf(Array);
          (data.length).should.be.exactly(1);
          should(data[0]).have.properties([ 'Ordernum', 'Linenum', 'Qty' ]);
          (data[0].Ordernum).should.be.exactly(10);
          (data[0].Linenum).should.be.exactly(1);
          done();
        } catch (err) {
          done(err);
        }

      }
    }));
  });

  it('should count customers', function(done) {
    this.timeout(10000);

    akera.doCount({
      broker : broker,
      params : {
        db : 'sports2000',
        table : 'customer'
      }
    }, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          (data.count).should.be.above(1);
          done();
        } catch (err) {
          done(err);
        }

      }
    }));
  });

  it('should count customers with filter', function(done) {
    this.timeout(10000);

    akera.doCount({
      broker : broker,
      params : {
        db : 'sports2000',
        table : 'customer'
      },
      query : {
        filter : {
          where : {
            CustNum : {
              lt : 5
            }
          }
        }
      }
    }, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          (data.count).should.be.exactly(4);
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

    akera.doCreate({
      broker : broker,
      params : {
        db : 'sports2000',
        table : 'progress'
      },
      body : {
        Name : name,
        City : 'Cluj'
      }
    }, new Response(function(err, data) {
      if (err)
        done();
      else
        done(new Error('Should have failed for invalid table'))
    }));
  });

  it('should create customer record', function(done) {
    this.timeout(10000);

    var name = 'Medu - ' + new Date().toString();

    akera.doCreate({
      broker : broker,
      params : {
        db : 'sports2000',
        table : 'customer'
      },
      body : {
        Name : name,
        City : 'Cluj'
      }
    }, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
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

  it('should fail to update customer record if not pk values set', function(
    done)
  {
    this.timeout(10000);

    var addr = 'Cluj - ' + new Date().toString();

    akera.doUpdate({
      broker : broker,
      params : {
        db : 'sports2000',
        table : 'customer'
      // cust num, pk value
      },
      body : {
        Address2 : addr,
        Balance : 8080
      }
    }, new Response(function(err, data) {
      if (err)
        done();
      else
        done(new Error('Update should have failed with invalid pk values.'))
    }));
  });

  it('should update customer record', function(done) {
    this.timeout(10000);

    var addr = 'Cluj - ' + new Date().toString();

    akera.doUpdate({
      broker : broker,
      params : {
        db : 'sports2000',
        table : 'customer',
        0 : 5
      // cust num, pk value
      },
      body : {
        Address2 : addr,
        Balance : 8080
      }
    }, new Response(
      function(err, data) {
        if (err)
          done(new Error(err.message));
        else {
          try {
            should(data).be.an.instanceOf(Object);
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

  it('should update customer record by rowid', function(done) {
    this.timeout(10000);

    var addr = 'Cluj - ' + new Date().toString();

    akera.doUpdateByRowid({
      broker : broker,
      params : {
        db : 'sports2000',
        table : 'customer',
        id : '0x0000000000000061' // customer rowid value
      },
      body : {
        Address2 : addr,
        Balance : 9898
      }
    }, new Response(
      function(err, data) {
        if (err)
          done(new Error(err.message));
        else {
          try {
            should(data).be.an.instanceOf(Object);
            should(data[0]).have
              .properties([ 'CustNum', 'Balance', 'Address2' ]);
            (data[0].Address2).should.be.exactly(addr);
            (data[0].Balance).should.be.exactly(9898);
            done();
          } catch (err) {
            done(err);
          }

        }
      }));
  });

  it('should delete customer record', function(done) {
    this.timeout(10000);

    akera.doDelete({
      broker : broker,
      params : {
        db : 'sports2000',
        table : 'customer',
        0 : newCustomer
      // cust num, pk value
      }
    }, new Response(function(err, data) {
      if (err)
        done(new Error(err.message));
      else {
        try {
          (data.updated).should.be.exactly(1);
          done();
        } catch (err) {
          done(err);
        }

      }
    }));
  });

  it('should fail to delete customer record if not all pk values set',
    function(done) {
      this.timeout(10000);

      akera.doDelete({
        broker : broker,
        params : {
          db : 'sports2000',
          table : 'customer'
        // cust num, pk value
        }
      }, new Response(function(err, data) {
        if (err)
          done();
        else
          done(new Error('Delete should have failed for missing pk values'));
      }));
    });

  it('should fail to delete customer record by rowid', function(done) {
    this.timeout(10000);

    akera.doDeleteByRowid({
      broker : broker,
      params : {
        db : 'sports2000',
        table : 'customer',
        id : '0x0000000000000061' // customer rowid value
      }
    }, new Response(function(err, data) {
      if (err)
        done();
      else
        done(new Error('Delete should have failed due to trigger validation'));
    }));
  });

});
