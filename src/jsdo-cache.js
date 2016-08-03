

function JSDOCacheManager() {
    var _services = [];
    this._rootLoaded = false;
    
    this.getAllServices = function() {
      return _services;
    };
    
    this.getService = function(svName) {
      for (var i=0; i<_services.length; i++) {
        var s = _services[i];
        if (s.name === svName) {
          return s;
        }
      }
      return null;
    };
    
    this.storeService = function(svc) {
      var s = this.getService(svc.name);
      if (s) {
        this.concatenateResources(s, svc);
      } else {
        _services.push(svc);
      }
    };
    
    this.getTableResource = function(svName, tableName) {
      var s = this.getService(svName);
      if (s) {
        for (var i=0; i<s.resources.length; i++) {
          var resource = s.resources[i];
          if (resource.name === tableName) {
            return resource;
          }
        }
        return null;
      }
      return null;
    };
    
    this.storeTableResource = function(svName, table) {
      var s = this.getService(svName);
      if (!s) {
        s = {
          name: svName,
          address: '\/' + svName.split('Service')[0],
          useRequest: true,
          resources: []
        };
        _services.push(s);
      }
      
      s.resources.push(table);
    };
    
    this.concatenateResources = function(service1, service2) {
      for (var i=0; i<service2.resources.length; i++) {
        var r2 = service2.resources[i];
        var exists = false;
        for (var j=0; j<service1.resources.length; j++) {
          var r1 = service1.resources[j];
          if (r1.name === r2.name) {
            exists = true;
            break;
          }
        }
        if (!exists) {
          service1.resources.push(r2);
        }
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

module.exports = JSDOCacheManager;