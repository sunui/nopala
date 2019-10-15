const thrift =require('thrift');
const  { Q } =thrift;
const types =require('./impala/_thrift_gen/beeswax_types') ;
const service = require('./impala/_thrift_gen/ImpalaService');

/**
 * The class contains essential functions for executing queries
 * via Beeswax Service.
 */
class ImpalaClient {

	/**
	 * Creates connection using given props.
	 *
	 * @param props {object}
	 * @param callback {function}
	 * @returns {function|promise}
	 */
	connect(props = {}, callback) {
	  this.host = props.host || '127.0.0.1';
	  this.port = props.port || 21000;
	  this.resultType = props.resultType || null;
	  this.timeout = props.timeout || 1000;
	  this.transport = props.transport || thrift.TBufferedTransport;
	  this.protocol = props.protocol || thrift.TBinaryProtocol;
	  this.options = {
		transport: this.transport,
		protocol: this.protocol,
		timeout: this.timeout
	  };
  
	  const deferred = Q.defer();
  
	  const connection = thrift.createConnection(this.host, this.port, this.options);
	  const client = thrift.createClient(service, connection);
  
	  connection.on('error', ImpalaClient.onErrorDeferred(deferred));
  
	  connection.on('connect', () => {
		deferred.resolve('Connection is established.');
	  });
  
	  this.client = client;
	  this.connection = connection;
  
	  deferred.promise.nodeify(callback);
	  return deferred.promise;
	}
  
	/**
	 * Closes the current connection.
	 *
	 * @param callback {function}
	 * @returns {function|promise}
	 */
	close(callback) {
	  const deferred = Q.defer();
	  const conn = this.connection;
  
	  if (!conn) {
		deferred.reject(new Error('Connection was not created.'));
	  } else {
		conn.end();
		deferred.resolve('Connection has ended.');
	  }
  
	  deferred.promise.nodeify(callback);
	  return deferred.promise;
	}
  
	/**
	 * Gets the query plan for a query.
	 *
	 * @param sql {string}
	 * @param callback {function}
	 * @returns {function|promise}
	 */
	explain(sql, callback) {
	  const deferred = Q.defer();
	  const query = ImpalaClient.createQuery(sql);
	  const client = this.client;
  
	  if (!client) {
		deferred.reject(new Error('Connection was not created.'));
	  } else {
		client.explain(query)
		  .then((explanation) => {
			deferred.resolve(explanation);
		  })
		  .catch(err => deferred.reject(err));
	  }
  
	  deferred.promise.nodeify(callback);
	  return deferred.promise;
	}
  
	/**
	 * Gets the result metadata.
	 *
	 * @param sql {string}
	 * @param callback {function}
	 * @returns {function|promise}
	 */
	getResultsMetadata(sql, callback) {
	  const deferred = Q.defer();
	  const query = ImpalaClient.createQuery(sql);
	  const client = this.client;
  
	  if (!client) {
		deferred.reject(new Error('Connection was not created.'));
	  } else {
		client.query(query)
		  .then(handle =>
			[handle, client.get_results_metadata(handle)]
		  )
		  .spread((handle, metaData) => {
			deferred.resolve(metaData);
  
			return handle;
		  })
		  .then((handle) => {
			client.clean(handle.id);
			client.close(handle);
		  })
		  .catch(err => deferred.reject(err));
	  }
  
	  deferred.promise.nodeify(callback);
	  return deferred.promise;
	}
  
	/**
	 * Transmits SQL command and receives result via Beeswax Service.
	 * The query runs asynchronously.
	 *
	 * @param sql {string}
	 * @param callback {function}
	 * @returns {function|promise}
	 */
	query(sql, callback) {
	  const deferred = Q.defer();
	  const query = ImpalaClient.createQuery(sql);
	  const resultType = this.resultType;
	  const client = this.client;
	  const connection = this.connection;
  
	  if (!client || !connection) {
		deferred.reject(new Error('Connection was not created.'));
	  } else {
		// increase the maximum number of listeners by 1
		// while this promise is in progress
		connection.setMaxListeners(connection.getMaxListeners() + 1);
  
		const onErrorCallback = ImpalaClient.onErrorDeferred(deferred);
		connection.on('error', onErrorCallback);
  
		client.query(query)
		  .then(handle =>
			[handle, client.get_state(handle)]
		  )
		  .spread((handle, state) =>
			[handle, state, client.fetch(handle)]
		  )
		  .spread((handle, state, result) =>
			[handle, state, result, client.get_results_metadata(handle)]
		  )
		  .spread((handle, state, result, metaData) => {
			const schemas = metaData.schema.fieldSchemas;
			const data = result.data;
			const processedData = ImpalaClient.processData(data, schemas, resultType);
  
			deferred.resolve(processedData);
  
			return handle;
		  })
		  .then((handle) => {
			client.clean(handle.id);
  
			// this promise is done, so we lower the maximum number of listeners
			connection.setMaxListeners(connection.getMaxListeners() - 1);
			connection.removeListener('error', onErrorCallback);
  
			client.close(handle);
		  })
		  .catch(err => deferred.reject(err));
	  }
  
	  deferred.promise.nodeify(callback);
	  return deferred.promise;
	}
  }
  
  /**
   * Creates Beeswax type query object.
   *
   * @param sql {string} SQL statement as string or Beeswax query object.
   * @returns {object}
   */
  ImpalaClient.createQuery = (sql) => {
	if (sql instanceof types.Query) {
	  return sql;
	}
  
	return new types.Query({ query: sql });
  };
  
  /**
   * Processes then returns the data according to desired type.
   *
   * @param data {object}
   * @param schemas {object}
   * @param type {string}
   * @returns {object} processed as the given data.
   */
  ImpalaClient.processData = (data, schemas, type) => {
	switch (type) {
	  case 'map': {
		let resultArray = [];
		const map = new Map();
  
		for (let i = 0; i < schemas.length; i += 1) {
		  resultArray = [];
		  for (let j = 0; j < data.length; j += 1) {
			resultArray.push(data[j].split('\t')[i]);
		  }
		  map.set(schemas[i].name, resultArray);
		}
  
		return map;
	  }
	  case 'json-array': {
		let resultObject = {};
		const array = [];
		const schemaNames = [];
  
		for (let i = 0; i < schemas.length; i += 1) {
		  schemaNames.push(schemas[i].name);
		}
  
		for (let i = 0; i < data.length; i += 1) {
		  resultObject = {};
		  for (let j = 0; j < schemaNames.length; j += 1) {
			resultObject[schemaNames[j]] = data[i].split('\t')[j];
		  }
		  array.push(resultObject);
		}
  
		return array;
	  }
	  case 'boolean': {
		return (data !== undefined) && (schemas !== undefined);
	  }
	  default: {
		return [data, schemas];
	  }
	}
  };
  
  ImpalaClient.onErrorDeferred = deferred => (err) => {
	deferred.reject(err);
  };

// eslint-disable-next-line import/prefer-default-export
module.exports.createClient = props => new ImpalaClient(props);
