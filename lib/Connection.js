
const createBeeswaxConnection = require('./protocol/beeswax.js')
const createHiveConnection = require('./protocol/hiveserver2.js')
var Events = require('events');

class Connection extends Events.EventEmitter {
	constructor(config) {
		super()
		this.config = config;

		if (!config.protocol) {
			this.config.protocol = 'beeswax'
		}
		this.state = 'disconnected';
		if (config.protocol === 'hiveserver2') {
			this.connection = createHiveConnection(config);
		} else if (config.protocol === 'beeswax') {
			this.connection = createBeeswaxConnection(config);
		} else {
			throw new Error('Unsupported protocol');
		}
	}

	processData(data, schemas) {
		switch (this.config.resultType) {
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
					schemaNames.push(schemas[i].columnName);
				}

				for (let i = 0; i < data.length; i += 1) {
					resultObject = {};
					for (let j = 0; j < schemaNames.length; j += 1) {
						resultObject[schemaNames[j]] = data[i][j];
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

	connect() {
		return this.connection.connect();
	}

	async query(sql) {
		return this.connection.query(sql, this.processData.bind(this))
	}

	close() {
		return this.connection.close()
	}
}


module.exports = Connection
