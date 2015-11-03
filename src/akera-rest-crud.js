module.exports = AkeraCrud;

var crud_router = require('./crud-router.js');

function AkeraCrud(akeraWebInstance) {
    if (akeraWebInstance && akeraWebInstance.app) {
        this.akeraWebInstance = akeraWebInstance;
    } else {
        throw new Error('Invalid akera web application instance');
    }
}

AkeraCrud.prototype.init = function(brokerName, route) {
    var app = this.akeraWebInstance.app;

    route = (route === '/' ? '/rest' : route) || '/rest';

    app.use(route + (brokerName ? '/' + brokerName : '/:broker'), new crud_router(brokerName || null, this.akeraWebInstance));
    this.log('info', 'Akera CRUD Service enabled for all brokers.');
};

AkeraCrud.prototype.log = function(level, message) {
    try {
        this.akeraWebInstance.log(level, message);
    } catch (err) {}
};
