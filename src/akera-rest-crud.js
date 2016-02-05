module.exports = AkeraCrud;

var crud_router = require('./crud-router.js');

function AkeraCrud() {
}

AkeraCrud.prototype.init = function(config, router) {
    crud_router(router);
};
