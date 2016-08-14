var should = require('should');
var AkeraHandler = require('../src/akera/handler.js');
var akera = new AkeraHandler();
var crud = akera.getDataAccess();

var broker = {
  alias : 'sports',
  host : '10.10.10.6',
  port : 38900
};

var badBroker = {
  alias : 'sports',
  host : '10.10.10.6',
  port : 33333
};

describe('Akera Crud', function() {

  // create tests
  it('create should fail if invalid broker', function(done) {
    this.timeout(10000);

    crud.create(badBroker, 'sports2000.Customer', {
      'Name' : 'Medu ' + new Date().toString(),
      'City' : 'Cluj'
    }).then(function(info) {
      done(new Error('Create should have failed but returned: ' + info));
    }, function(err) {
      done();
    });

  });

  it('create should fail if invalid table', function(done) {
    this.timeout(10000);

    crud.create(broker, 'Progress', {
      'Name' : 'Medu ' + new Date().toString(),
      'City' : 'Cluj'
    }).then(function(info) {
      done(new Error('Create should have failed but returned: ' + info));
    }, function(err) {
      done();
    });

  });

  it('create should fail if invalid field', function(done) {
    this.timeout(10000);

    crud.create(broker, 'sports2000.Customer', {
      'Name' : 'Medu ' + new Date().toString(),
      'Progress' : 'Cluj'
    }).then(function(info) {
      done(new Error('Create should have failed but returned: ' + info));
    }, function(err) {
      done();
    });

  });

  it('create should set correct values', function(done) {
    this.timeout(10000);
    var name = 'Medu ' + new Date().toString();

    crud.create(broker, 'sports2000.Customer', {
      'Name' : name,
      'City' : 'Cluj',
      'Balance' : 1024
    }).then(function(info) {
      try {
        should(info).be.an.instanceOf(Object);
        (info.City).should.be.exactly('Cluj');
        (info.Balance).should.be.exactly(1024);
        (info.Name).should.be.exactly(name);
        done();
      } catch (err) {
        done(err);
      }
    }, done);

  });

  // read tests
  it('read should fail if invalid broker', function(done) {
    this.timeout(10000);

    crud.read(badBroker, 'sports2000.Customer').then(function(info) {
      done(new Error('Read should have failed but returned: ' + info));
    }, function(err) {
      done();
    });

  });

  it('read should fail if invalid table', function(done) {
    this.timeout(10000);

    crud.read(broker, 'Progress').then(function(info) {
      done(new Error('Read should have failed but returned: ' + info));
    }, function(err) {
      done();
    });

  });

  it('read should fail if invalid filter', function(done) {
    this.timeout(10000);

    crud.read(broker, 'sports2000.Customer', {
      where : {
        'CustNumber' : 2
      }
    }).then(function(info) {
      done(new Error('Read should have failed but returned: ' + info));
    }, function(err) {
      done();
    });

  });

  it('read should work with pk filter', function(done) {
    this.timeout(10000);

    crud.read(broker, 'sports2000.Customer', {
      pk : {
        'CustNum' : 2
      }
    }).then(function(info) {
      try {
        should(info).be.an.instanceOf(Array);
        (info.length).should.be.exactly(1);
        (info[0].CustNum).should.be.exactly(2);

        done();
      } catch (err) {
        done(err);
      }
    }, done);

  });

  it('read should work with where filter', function(done) {
    this.timeout(10000);

    crud.read(broker, 'sports2000.Customer', {
      where : {
        'CustNum' : {
          'lt' : 3
        }
      }
    }).then(function(info) {
      try {
        should(info).be.an.instanceOf(Array);
        (info.length).should.be.exactly(2);
        (info[0].CustNum).should.be.exactly(1);
        (info[1].CustNum).should.be.exactly(2);
        done();
      } catch (err) {
        done(err);
      }
    }, done);

  });

  it('read should work with where filter and pk', function(done) {
    this.timeout(10000);

    crud.read(broker, 'sports2000.Customer', {
      where : {
        'CustNum' : {
          'gt' : 3
        }
      },
      pk : {
        'CustNum' : 2
      }
    }).then(function(info) {
      try {
        should(info).be.an.instanceOf(Array);
        (info.length).should.be.exactly(0);
        done();
      } catch (err) {
        done(err);
      }
    }, done);

  });

  it('read should work with pagination', function(done) {
    this.timeout(10000);

    crud.read(broker, 'sports2000.Customer', {
      where : {
        'CustNum' : {
          'lt' : 10
        }
      },
      top : 2,
      offset : 3
    }).then(function(info) {
      try {
        should(info).be.an.instanceOf(Array);
        (info.length).should.be.exactly(2);
        (info[0].CustNum).should.be.exactly(3);
        (info[1].CustNum).should.be.exactly(4);
        done();
      } catch (err) {
        done(err);
      }
    }, done);

  });

  it('read should work with sort descending', function(done) {
    this.timeout(10000);

    crud.read(broker, 'sports2000.Customer', {
      where : {
        'Name' : {
          'lt' : 'b'
        }
      },
      top : 2,
      sort : [ 'Name' ]
    }).then(function(info) {
      try {
        should(info).be.an.instanceOf(Array);
        (info.length).should.be.exactly(2);
        (info[0].Name.localeCompare(info[1].Name)).should.be.below(0);
        done();
      } catch (err) {
        done(err);
      }
    }, done);

  });

  it('read should work with sort descending', function(done) {
    this.timeout(10000);

    crud.read(broker, 'sports2000.Customer', {
      where : {
        'Name' : {
          'lt' : 'b'
        }
      },
      top : 2,
      sort : [ {
        'Name' : true
      } ]
    }).then(function(info) {
      try {
        should(info).be.an.instanceOf(Array);
        (info.length).should.be.exactly(2);
        (info[0].Name.localeCompare(info[1].Name)).should.be.above(0);
        done();
      } catch (err) {
        done(err);
      }
    }, done);

  });

  // update tests
  it('update should fail if invalid broker', function(done) {
    this.timeout(10000);

    crud.update(badBroker, 'sports2000.Customer', {
      'CustNum' : 24,
      'Name' : 'Lift'
    }, {
      'City' : 'Cluj'
    }).then(function(info) {
      done(new Error('Update should have failed but returned: ' + info));
    }, function(err) {
      done();
    });

  });

  it('update should fail if no record found', function(done) {
    this.timeout(10000);

    crud.update(broker, 'sports2000.Customer', {
      'CustNum' : 24,
      'Name' : 'Lift'
    }, {
      'City' : 'Cluj'
    }).then(function(info) {
      done(new Error('Update should have failed but returned: ' + info));
    }, function(err) {
      done();
    });

  });

  it('update should fail if invalid table', function(done) {
    this.timeout(10000);

    crud.update(broker, 'Progress', {
      'CustNum' : 24,
      'Name' : 'Lift'
    }, {
      'City' : 'Cluj'
    }).then(function(info) {
      done(new Error('Update should have failed but returned: ' + info));
    }, function(err) {
      done();
    });

  });

  it('update should fail if invalid filter', function(done) {
    this.timeout(10000);

    crud.update(broker, 'sports2000.Customer', {
      'CustNum' : 24,
      'Progress' : 'Lift'
    }, {
      'City' : 'Cluj'
    }).then(function(info) {
      done(new Error('Update should have failed but returned: ' + info));
    }, function(err) {
      done();
    });

  });

  it('update should set correct values', function(done) {
    this.timeout(10000);

    crud.update(broker, 'sports2000.Customer', {
      'CustNum' : 24
    }, {
      'City' : 'Cluj',
      'Balance' : 1024
    }).then(function(info) {
      should(info).be.an.instanceOf(Array);
      (info.length).should.be.exactly(1);
      (info[0].City).should.be.exactly('Cluj');
      (info[0].Balance).should.be.exactly(1024);
      (info[0].CustNum).should.be.exactly(24);
      done();
    }, done);

  });

  // delete tests
  it('delete should fail if invalid broker', function(done) {
    this.timeout(10000);

    crud.destroy(badBroker, 'sports2000.Customer', {
      'CustNum' : 24,
      'Name' : 'Lift'
    }).then(function(info) {
      done(new Error('Delete should have failed but returned: ' + info));
    }, function(err) {
      done();
    });

  });

  it('delete should fail if invalid table', function(done) {
    this.timeout(10000);

    crud.destroy(broker, 'Progress', {
      'CustNum' : 24,
      'Name' : 'Lift'
    }).then(function(info) {
      done(new Error('Delete should have failed but returned: ' + info));
    }, function(err) {
      done();
    });

  });

  it('delete should fail if invalid filter', function(done) {
    this.timeout(10000);

    crud.destroy(broker, 'sports2000.Customer', {
      'CustNum' : 24,
      'Progress' : 'Lift'
    }).then(function(info) {
      done(new Error('Delete should have failed but returned: ' + info));
    }, function(err) {
      done();
    });

  });

  it('delete should return zero if no record found', function(done) {
    this.timeout(10000);

    crud.destroy(broker, 'sports2000.Customer', {
      'CustNum' : 24,
      'Name' : 'Lift'
    }).then(function(info) {
      try {
        (info).should.be.exactly(0);
        done();
      } catch (err) {
        done(err);
      }
    }, done);

  });

  it('delete should fail when delete validation', function(done) {
    this.timeout(10000);

    crud.destroy(broker, 'sports2000.Customer', {
      'CustNum' : 24
    }).then(function(info) {
      done(new Error('Delete should have failed but returned: ' + info));
    }, function(err) {
      done();
    });

  });

});
