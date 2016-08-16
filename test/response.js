function Response(cb) {
  this.cb = cb;

  this.status = function(code) {
    this.code = code;

    return this;
  };

  this.send = function(data) {
    if (this.code === 200)
      cb(null, data);
    else
      cb(data);
    return this;
  };

}

module.exports = Response;
