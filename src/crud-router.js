var akeraApi = require('akera-api');
var dbsList = null;
var dbsTables = [];
var tblFields = [];

function setupRouter(router) {
	var broker = router.__broker;

	router.get('/meta', function(req, res) {
		connect(broker, function(err, conn) {
			if (err) {
				sendError(err, res);
				return;
			}
			listDatabases(conn, function(err, dbs) {
				if (err) {
					sendError(err, res);
					conn.disconnect();
					return;
				}
				res.status(200).send(getDbsNames(dbs));
				conn.disconnect();
			});
		});
	});

	router.get('/meta/:database', function(req, res) {
		connect(broker, function(err, conn) {
			if (err) {
				sendError(err, res);
				return;
			}
			var params = {
				db: req.params.database,
				conn: conn
			};
			listTables(params, function(err, tables) {
				if (err) {
					sendError(err, res);
					conn.disconnect();
					return;
				}
				var tblNames = [];
				for (var key in tables) {
					tblNames.push(tables[key].getName());
				}
				res.status(200).send(tblNames);
				conn.disconnect();
			});
		});
	});

	router.get('/meta/:database/:table', function(req, res) {
		connect(broker, function(err, conn) {
			if (err) {
				sendError(err, res);
				return;
			}
			var params = {
				table: req.params.table,
				db: req.params.database,
				conn: conn
			};
			listFields(params, function(err, fields) {
				if (err) {
					sendError(err, res);
					conn.disconnect();
					return;
				}
				res.status(200).send(fields);
				conn.disconnect();
			});
		});
	});

	router.get('/meta/:database/:table/indexes', function(req, res) {
		connect(broker, function(err, conn) {
			if (err) {
				sendError(err, res);
				return;
			}
			var params = {
				table: req.params.table,
				db: req.params.database,
				conn: conn
			};
			listTables(params, function(err, tables) {
				if (err) {
					sendError(err, res);
					conn.disconnect();
					return;
				}
				var selTbl;
				for (var key in tables) {
					if (params.table === tables[key].getName()) {
						selTbl = tables[key];
					}
				}
				if (selTbl) {
					selTbl.getAllIndexes().then(function(indexes) {
						res.status(200).send(indexes);
						conn.disconnect();
					}, function(err) {
						sendError(err, res);
						conn.disconnect();
					});
				}
			});
		});
	});

	router.get('/:db/:table', function(req, res) {
		connect(broker, req.params.db, function(err, conn) {
			if (err) {
				sendError(err, res);
				return;
			}
			try {
				var query = getQuery(req);
				var q = setupWhereFilter(conn, query);
				q.all().then(function(rsp) {
					res.status(200).send(rsp);
					conn.disconnect();
				}, function(err) {
					sendError(err, res);
					conn.disconnect();
				});
			} catch (e) {
				sendError(e, res);
				conn.disconnect();
				return;
			}
		});
	});

	router.get('/:db/:table/count', function(req, res) {
		connect(broker, req.params.db, function(err, conn) {
			if (err) {
				sendError(err, res);
				return;
			}
			try {
				var query = getQuery(req);
				var q = setupWhereFilter(conn, query);
				q.count().then(function(rsp) {
					rsp = {
						num: rsp
					};
					res.status(200).send(rsp);
					conn.disconnect();
				}, function(err) {
					sendError(err, res);
					conn.disconnect();
				});
			} catch (e) {
				sendError(e, res);
				conn.disconnect();
			}
		});
	});
	//insert
	router.post('/:db/:table', function(req, res) {
		connect(broker, req.params.db, function(err, conn) {
			if (err) {
				sendError(err, res);
				return;
			}
			try {
				var table = req.params.table;
				var data = getData(req);
				conn.query.insert(table)
					.set(data)
					.fetch().then(function(inserted) {
						res.status(200).send(inserted);
						conn.disconnect();
					}, function(err) {
						sendError(err, res);
						conn.disconnect();
					});
			} catch (e) {
				sendError(e, res);
				conn.disconnect();
			}
		});
	});
	//upsert
	router.put('/:db/:table', function(req, res) {
		connect(broker, req.params.db, function(err, conn) {
			if (err) {
				sendError(err, res);
				return;
			}
			try {
				var data = getData(req);
				conn.query.upsert(req.params.table)
					.set(data)
					.fetch().then(function(inserted) {
						res.status(200).send(inserted);
						conn.disconnect();
					}, function(err) {
						sendError(err, res);
						conn.disconnect();
					});
			} catch (e) {
				sendError(e, res);
				conn.disconnect();
			}
		});
	});

	router.post('/:db/:table/update', function(req, res) {
		connect(broker, req.params.db, function(err, conn) {
			if (err) {
				sendError(err, res);
				return;
			}
			try {
				var qry = getQuery(req);
				var data = getData(req);
				var q = conn.query.update(qry.table);
				if (!qry.filter.where) {
					throw new Error('Invalid filter where condition');
				}
				q.where(qry.filter.where);
				q.set(data)
					.fetch().then(function(updated) {
						res.status(200).send(updated);
						conn.disconnect();
					}, function(err) {
						sendError(err, res);
						conn.disconnect();
					});
			} catch (e) {
				sendError(e, res);
				conn.disconnect();
			}
		});
	});

	router.delete('/:db/:table', function(req, res) {
		connect(broker, req.params.db, function(err, conn) {
			if (err) {
				sendError(err, res);
				return;
			}
			try {
				var qry = getQuery(req);
				var q = conn.query.destroy(qry.table);
				if (!qry.filter.where) {
					throw new Error('Invalid filter where condition');
				}
				q.where(qry.filter.where)
					.go().then(function(deleted) {
						res.status(200).send({
							deleted: deleted
						});
						conn.disconnect();
					}, function(err) {
						sendError(err, res);
						conn.disconnect();
					});
			} catch (e) {
				sendError(e, res);
				conn.disconnect();
			}
		});
	});
	//TODO: implement CRUD request handles
	return router;
}

function listDatabases(conn, callback) {
	var md = conn.getMetaData();
	if (dbsList) {
		callback(null, dbsList);
	}
	md.allDatabases().then(function(dbs) {
		dbsList = dbs;
		callback(null, dbsList);
	}, function(err) {
		callback(err);
	});
}

function listTables(params, callback) {
	var qryDbs = params.db;
	var conn = params.conn;
	listDatabases(conn, function(err, dbs) {
		if (err) {
			callback(err);
			return;
		}
		var selectedDbs = null;
		for (var k in dbs) {
			if (qryDbs === dbs[k].getLname().toString()) {
				selectedDbs = dbs[k];
			}
		}
		if (!selectedDbs) {
			callback('Cannot find tables for database : ' + qryDbs);
			return;
		}
		for (var key in dbsTables) {
			if (selectedDbs.getLname() === dbsTables[key].db) {
				callback(null, dbsTables[key].tables);
				return;
			}
		}
		selectedDbs.allTables().then(function(tables) {
			var tblItem = {
				db: selectedDbs.getLname(),
				tables: tables
			};
			dbsTables.push(tblItem);
			callback(null, tables);
		}, function(err) {
			callback(err);
		});
	});
}

function listFields(params, callback) {
	for (var key in tblFields) {
		if (params.table === tblFields[key].table) {
			callback(null, tblFields[key].fields);
		}
	}
	listTables(params, function(err, tables) {
		if (err) {
			callback(err);
			return;
		}
		var selTable = null;
		for (var key in tables) {
			if (params.table === tables[key].getName()) {
				selTable = tables[key];
			}
		}
		if (!selTable) {
			callback('Cannot find table ' + params.table);
			return;
		}
		selTable.getAllFields().then(function(fields) {
			var tblField = {
				table: params.table,
				fields: fields
			};
			tblFields.push(tblField);
			callback(null, fields);
		}, function(err) {
			callback(err);
		});
	});
}

function getDbsNames(dbsList) {
	var dbsNames = [];
	for (var key in dbsList) {
		dbsNames.push(dbsList[key].getLname());
	}
	return dbsNames;
}

function sendError(err, res) {
	var e;
	if (err instanceof Object) {
		if (Object.keys(err).length !== 0) {
			e = JSON.stringify(err);
		} else {
			e = err.toString();
		}
	} else if (typeof(err) === 'string') {
		e = err.toString();
	}
	res.status(500).send(e);
}

function getQuery(req) {
	var qry = {};
	qry.table = req.params.table;
	if (!req.query.filter) {
		qry.filter = {};
	} else if (req.query.filter instanceof Object) {
		qry.filter = req.query.filter;
	} else {
		try {
			qry.filter = JSON.parse(req.query.filter);
		} catch (err) {
			throw new Error('Filter type must be JSON');
		}
	}
	return qry;
}

function getData(req) {
	console.log(req.body);
	if (req.body instanceof Object) {
		var data = req.body;
		if (Object.keys(data).length === 0) {
			throw new Error('No data to insert');
		}
		return data;
	} else {
		throw new Error('Data type must be JSON');
	}
}

function connect() {
	var broker, database, callback = null;
	if (arguments.length === 2) {
		broker = arguments[0];
		callback = arguments[1];
	} else if (arguments.length === 3) {
		broker = arguments[0];
		database = arguments[1];
		callback = arguments[2];
	}
	akeraApi.connect(broker).then(function(conn) {
		conn.autoReconnect = true;
		if (database) {
			conn.selectDatabase(database).then(function() {
				callback(null, conn);
			}, function(err) {
				callback(err);
			});
		} else {
			callback(null, conn);
		}
	}, function(err) {
		callback(err);
		conn.disconnect();
	});
}

function setupWhereFilter(conn, query) {
	//build the query
	var q = conn.query.select(query.table);
	//defaults fields to *
	q.fields();
	for (var key in query.filter) {
		if (q[key]) {
			q[key](query.filter[key]);
		}
	}
	return q;
}

module.exports = setupRouter;
