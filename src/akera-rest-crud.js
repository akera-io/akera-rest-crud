module.exports = AkeraRestCrud;

var akeraApp = null;

function AkeraRestCrud(akeraWebApp) {
  var self = this;
  var _akeraHandler = null;

  this.error = function(err, res) {
    if (err) {
      if (err instanceof Error) {
        err = err.message;
      }

      res.status(500).send({
        message : err
      });

      if (akeraApp)
        akeraApp.log('error', err);
    }
  };

  this.getAkeraHandler = function() {
    if (!_akeraHandler) {
      var AkeraHandler = require('./akera/handler.js');
      _akeraHandler = new AkeraHandler(self);
    }
    return _akeraHandler;
  };

  this.setupInterface = function(type, config, router) {
    type = type || 'rest';

    switch (type) {
      case 'odata':
        // TODO: implement odata support
        break;
      case 'jsdo':
        var JSDOHandler = require('./jsdo/handler.js');

        var jsdoHndl = new JSDOHandler(self);
        jsdoHndl.init(config, router);
        break;
      case 'rest':
        var akeraHndl = self.getAkeraHandler();
        akeraHndl.init(config, router);
        break;
      default:
        throw new Error('Invalid api interface specified');
    }
  };

  this.init = function(config, router) {
    var self = this;
    if (!router || !router.__app || typeof router.__app.require !== 'function')
    {
      throw new Error('Invalid Akera web service router.');
    }

    config = config || {};
    akeraApp = router.__app;
    config.route = akeraApp.getRoute(config.route || '/rest/crud/');
    config.serviceInterface = config.serviceInterface || 'rest';

    if (config.serviceInterface instanceof Array) {
      if (config.serviceInterface.length === 0) {
        self.setupInterface('rest', config, router);
      } else {
        // Service interfaces need to be configured in order, otherwise path
        // mappings will conflict
        if (config.serviceInterface.indexOf('jsdo') >= 0) {
          self.setupInterface('jsdo', config, router);
        }
        if (config.serviceInterface.indexOf('odata') >= 0) {
          self.setupInterface('odata', config, router);
        }
        if (config.serviceInterface.indexOf('rest') >= 0) {
          self.setupInterface('rest', config, router);
        }
      }
    } else {
      self.setupInterface(config.serviceInterface, config, router);
    }

  };

  if (akeraWebApp !== undefined) {
    throw new Error(
      'Rest File service can only be mounted at the broker level.');
  }
}

AkeraRestCrud.init = function(config, router) {
  var restCrud = new AkeraRestCrud();
  restCrud.init(config, router);
};
