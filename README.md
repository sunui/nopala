# nopala
Impala and Hive client for Nodejs.
Support both of Beeswax and Hiveserver2.
Base on Thrift.

### TODO

* connection pool

### reference: 

* [impyla](https://github.com/cloudera/impyla) 
* [impala](https://github.com/cloudera/impala) 
* [impala-shell](https://github.com/dknupp/impala-shell) 
* [mysqljs/mysql](https://github.com/mysqljs/mysql) 

### Dependencies

* thrift

### versions

* Nodejs v10
* Impala cdh6.3.0
* hive release-2.3.3
* Thrift 0.12.0

### examples

```bash
npm install nopala
```

#### hs2
```js
import {createConnection} from 'nopala';

const connection = createConnection({
	protocol:'hiveserver2',
	host: '0.0.0.0',
	port: '21050',
});

connection.connect();

connection.query(`select * from db.tb limit 1;`).then(result=>{
	console.log(result);
}).finally(()=>{
	connection.close();
})
```

#### beeswax

```js
import {createConnection} from 'nopala';

const connection = createConnection({
  protocol: 'beeswax'
  host: '0.0.0.0',
  port: 21000,
});

connection.connect();

connection.query(`select * from db.tb limit 1;`).then(result=>{
	console.log(result);
}).finally(()=>{
	connection.close();
})
```

