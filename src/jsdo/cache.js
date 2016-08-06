function JSDOCacheManager() {
  var _services = [];
  var _fullyLoaded = [];

  this._rootLoaded = false;
  this.lastModified = new Date().toString();

  this.getAllServices = function() {
    return _services;
  };

  this.getService = function(svName, tableName) {
    var service = _services && _services.filter(function(srv) {
      return srv.name === svName;
    });

    if (service) {
      if (!tableName) {
        return s;
      } else {
        var sTable = s.resources.filter(function(res) {
          return res.name === tableName;
        });

        if (sTable) {
          var sCopy = clone(s);
          sCopy.resources = sTable;
          return sCopy;
        }
      }
    }

    return null;
  };

  this.storeService = function(svc, isFullyLoaded) {
    var s = this.getService(svc.name);
    if (s) {
      this.concatenateResources(s, svc);
    } else {
      _services.push(svc);
    }
    if (isFullyLoaded) {
      _fullyLoaded.push(svc.name);
    }

    // update last modified timestamp
    this.lastModified = new Date().toString();
  };

  this.isSvcFullyLoaded = function(svc) {
    return svc && _fullyLoaded.indexOf(svc.name || svc) >= 0;
  };

  this.getTableResource = function(svName, tableName) {
    var s = this.getService(svName);
    if (s) {
      return s.resources.filter(function(resource) {
        return resource.name === tableName;
      });
    }
    return null;
  };

  this.storeTableResource = function(svName, table) {
    var s = this.getService(svName);
    if (!s) {
      s = {
        name : svName,
        address : '\/' + svName,
        useRequest : true,
        resources : []
      };
      _services.push(s);
    }

    s.resources.push(table);
  };

  this.concatenateResources = function(service1, service2) {
    if (service1 && service2) {
      service2.resources.forEach(function(newRes) {
        if (!service1.resources.find(function(res) {
          return res.name === newRes.name;
        }))
        {
          service1.respources.push(newRes);
        }
      });
    }
  };

  this.rootLoaded = function(loaded) {
    if (loaded) {
      this._rootLoaded = loaded;
    } else {
      return this._rootLoaded;
    }
  };
}

function clone(o) {
  var ret = {};
  Object.keys(o).forEach(function(val) {
    ret[val] = o[val];
  });
  return ret;
}

module.exports = JSDOCacheManager;
