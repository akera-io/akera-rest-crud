[![Akera Logo](http://akera.io/logo.png)](http://akera.io/)

  REST database access module (CRUD) for Akera.io web service - used by rest explorer module 
  and other mobile/web clients this broker middleware service provides access to connected databases 
  on the application server. Both meta-data information (database structure) and create/read/update/delete 
  (CRUD) operations are supported. 
  
## Installation

```bash
$ npm install akera-rest-crud
```

## Docs

  * [Website and Documentation](http://akera.io/)

## Quick Start

  This module is designed to only be loaded as broker level service which 
  is usually done by adding a reference to it in `services` section of 
  each broker's configuration in `akera-web.json` configuration file.
   
```json
  "brokers": [
  	{	"name": "demo",
  		"host": "localhost",
		"port": 3737,
		"services": [
			{ 
				"middleware": "akera-rest-crud",
				"config": {
					"route": "/rest-crud/"
				}
			}
		]
	}
  ]
```
  
  Service options available:
	- `route`: the route where the service is going to be mounted (default: '/rest/crud/')
  
  The interface can then be used to retrieve meta-data information about connected databases, 
  create/read/update/delete records from those databases by making HTTP requests: 

| Method | Url | Function | Result |
| --- | --- | --- | --- |
| GET    | `http://[host]/[broker]/rest-crud/[db]/[table]` | reads records from database table, optional query parameters (filter, sort, limit, offset) can be sent using the `filter` parameter through the HTTP query string | HTTP 404 if no record found, JSON object if only one record found or array if multiple |
| GET    | `http://[host]/[broker]/rest-crud/[db]/[table]/[id](/[id])*` | reads a single record from database table, primary key field(s) values should be sent as URL segments in the same order as in the primary index definition | HTTP 404 if record not found, JSON object if found |
| GET    | `http://[host]/[broker]/rest-crud/[db]/[table]/count` | returns number of records from database table, optional query filter can be sent using the `filter` parameter through the HTTP query string | JSON object with `num` property holding the number of records |
| POST   | `http://[host]/[broker]/rest-crud/[db]/[table]` | create a new record in database table, the request body should be a JSON object holding values to be set for each table's fields | HTTP 500 if error on create, JSON object with full table record (fields might be set in database triggers) |
| PUT    | `http://[host]/[broker]/rest-crud/[db]/[table]/[id](/[id])*` | update a record in database table, primary key field(s) values should be sent as URL segments and the request body should be a JSON object holding values to be set for each table's fields | HTTP 404 if record not found, HTTP 500 if error on update, JSON object with full table record (fields might be updated in database triggers) |
| DELETE | `http://[host]/[broker]/rest-crud/[db]/[table]/[id](/[id])*` | delete a record from database table, primary key field(s) values should be sent as URL segments | HTTP 404 if record not found, HTTP 500 if error on delete, JSON object with `num` property holding the number of records deleted |
| GET    | `http://[host]/[broker]/rest-crud/meta` | returns the list of connected databases | JSON array with one string entry for each connected database containing the database logical name |
| GET    | `http://[host]/[broker]/rest-crud/meta/[db]` | returns the list of database's tables, `db` parameter can be either the database name or zero-base index | HTTP 404 if database not found, JSON array with one string entry for each database table containing the table name |
| GET    | `http://[host]/[broker]/rest-crud/meta/[db]/[table]` | returns database table's field information, `table` parameter can be either the database name or zero-base index | HTTP 404 if table not found, JSON array with one entry for each table field |
| GET    | `http://[host]/[broker]/rest-crud/meta/[db]/[table]/index` | returns database table's indexes information, `table` parameter can be either the database name or zero-base index | HTTP 404 if table not found, JSON array with one entry for each table index |
 
## License
	
[MIT](https://github.com/akera-io/akera-rest-crud/raw/master/LICENSE) 
