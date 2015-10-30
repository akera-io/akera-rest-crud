module.exports = AkeraCrud;

var crud_router = require('./crud-router.js');

function AkeraCrud(akeraWebInstance) {
    if (akeraWebInstance && akeraWebInstance.app) {
        this.akeraWebInstance = akeraWebInstance;
    } else {
        throw new Error('Invalid akera web application instance');
    }
}

AkeraCrud.prototype.init = function(brokers, route) {
    var app = this.akeraWebInstance.app;

    route = (route === '/' ? '/rest' : route) || '/rest';

    if (!brokers || brokers.length === 0) {
        app.use(route + '/:broker', new crud_router(null, this.akeraWebInstance));
        this.log('info', 'Akera CRUD Service enabled for all brokers.');
    } else {
        brokers.forEach(function(brokerName) {
            var broker_path = route + '/' + brokerName;
            app.use(broker_path, new crud_router(brokerName, this.akeraWebInstance));
            this.log('info', 'Akera CRUD Service enabled for broker\'' + brokerName);
        });
    }

    //this.brokers = config.brokers;
    /*
    app.use(broker_path, new file_router(brokerName, this.akeraWebInstance));
    app.use(broker_path, new crud_router(brokerName, this.akeraWebInstance));
    this.log('info', 'Akera rest service enabled for ' + brokerName || 'all defined brokers.');

    app.use(route, express.static(www_path));
    this.log('info', 'Akera rest explorer may be accessed via the following route: ' + route);
    */
};

AkeraCrud.prototype.log = function(level, message) {
    try {
        this.akeraWebInstance.log(level, message);
    } catch (err) {}
};
