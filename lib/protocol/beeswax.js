const thrift = require('thrift');
const types = require('../../impala/_thrift_gen/beeswax_types.js');
const service = require('../../impala/_thrift_gen/ImpalaService.js');

class BeeswaxConnection {
	constructor({ host = '127.0.0.1', port = 21000, timeout = 1000 } = {}) {
		this.host = host;
		this.port = port;
		this.timeout = timeout;
		this.transport = thrift.TBufferedTransport;
		this.protocol = thrift.TBinaryProtocol;
		this.options = {
			transport: this.transport,
			protocol: this.protocol,
			timeout: this.timeout
		};
	}
	static createQuery(sql) {
		if (sql instanceof types.Query) {
			return sql;
		}
		return new types.Query({ query: sql });
	}

	connect() {
		const connection = thrift.createConnection(this.host, this.port, this.options);
		const client = thrift.createClient(service, connection);

		this.client = client;
		this.connection = connection;

		return new Promise((resolve, reject) => {
			connection.on('error', reject);
			connection.on('connect', resolve);
		})
	}

	close() {
		const connection = this.connection;
		return new Promise((resolve, reject) => {
			if (!connection) {
				reject(new Error('Connection was not created.'));
			} else {
				connection.end();
				resolve('Connection has ended.');
			}
		})
	}

	explain(sql) {
		const query = BeeswaxConnection.createQuery(sql);
		const client = this.client;
		return client.explain(query)
	}

	getResultsMetadata(sql, callback) {
		const query = BeeswaxConnection.createQuery(sql);
		const client = this.client;
		return new Promise((resolve, reject) => {
			if (!client) {
				reject(new Error('Connection was not created.'));
			} else {
				client.query(query)
					.then(handle =>
						[handle, client.get_results_metadata(handle)]
					)
					.spread((handle, metaData) => {
						resolve(metaData);
						return handle;
					})
					.then((handle) => {
						client.clean(handle.id);
						client.close(handle);
					})
					.catch(err => reject(err));
			}

		})
	}

	query(sql, processData) {
		const query = BeeswaxConnection.createQuery(sql);
		const client = this.client;
		const connection = this.connection;
		return new Promise((resolve, reject) => {
			if (!client || !connection) {
				reject(new Error('Connection was not created.'));
			} else {
				// increase the maximum number of listeners by 1
				// while this promise is in progress
				connection.setMaxListeners(connection.getMaxListeners() + 1);

				connection.on('error', reject);

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
						const schemas = metaData.schema.fieldSchemas.map(column=>({...column,columnName:column.name}));
						const data = result.data.map(row=>row.split('\t'));
						if (processData) {
							resolve(processData(data, schemas));
						} else {
							resolve([data, schemas])
						}
						return handle;
					})
					.then((handle) => {
						client.clean(handle.id);

						// this promise is done, so we lower the maximum number of listeners
						connection.setMaxListeners(connection.getMaxListeners() - 1);
						connection.removeListener('error', reject);

						client.close(handle);
					})
					.catch(err => reject(err));
			}
		})
	}
}

module.exports = props => new BeeswaxConnection(props);
