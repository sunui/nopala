
const Connection = require("./lib/Connection")


exports.createConnection = config => new Connection(config)

exports.createPool = config => new Pool(config)

exports.createPoolCluster = () => { }

exports.createQuery = (sql, values, callback) => Connection.createQuery(sql, values, callback)

exports.escape = function escape(value, stringifyObjects, timeZone) {
	var SqlString = loadClass('SqlString');
  
	return SqlString.escape(value, stringifyObjects, timeZone);
  };