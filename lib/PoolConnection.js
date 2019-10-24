const Connection = require('./Connection');


class PoolConnection extends Connection {
	constructor(pool, options) {
		super(options);
		this._pool = pool;

		this.on('end', this._removeFromPool);
		this.on('error', function (err) {
			if (err.fatal) {
				this._removeFromPool();
			}
		});
	}

	release = () => {
		var pool = this._pool;

		if (!pool || pool._closed) {
			return undefined;
		}

		return pool.releaseConnection(this);
	}

	destroy = () => {
		Connection.destroy.apply(this, arguments);
		this._removeFromPool();
	}

	_realEnd = () => Connection.end;



	_removeFromPool = () => {
		if (!this._pool || this._pool._closed) {
			return;
		}

		var pool = this._pool;
		this._pool = null;

		pool._purgeConnection(this);
	}
}