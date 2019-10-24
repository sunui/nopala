const thrift = require('thrift');
const debug = require('debug')('jshs2:CConnection');
const HS2Util = require('./common/HS2Util');
// const Connection = require('./Connection');
const Timer = require('./common/Timer');
const HiveCursor = require('./HiveCursor');
const ConnectionError = require('./error/ConnectionError');

const TCLIService = require('../../../impala/_thrift_gen/TCLIService.js')
const TCLIServiceTypes = require('../../../impala/_thrift_gen/TCLIService_types.js')

const Configuration = require("./Configuration");

const AUTH_MECHANISMS = {
  NOSASL: {
    connection: thrift.createConnection,
    transport: thrift.TBufferedTransport,
  },
  PLAIN: {
    connection: thrift.createConnection,
    transport: thrift.TBufferedTransport,
  },
};

class HiveConnection {
  constructor(options) {
    const configure = new Configuration(options);


    this.conn = null;
    this.sessionHandle = null;
    this.thriftConnConfiguration = null;
    this.serverProtocolVersion = null;
    this.configure = configure;
    this.client = null;


    this.Configure = configure;
    this.SessionHandle = this.sessionHandle;
    this.ThriftConnConfiguration = this.thriftConnConfiguration
    this.ServerProtocolVersion = this.serverProtocolVersion
    this.Conn = this.conn
    this.Client = this.client

    this.connect = this.connect.bind(this);
    this.close = this.close.bind(this);
  }

  connect() {
    return new Promise((resolve, reject) => {
      debug(`OpenSession function start, -> ${this.configure.Host}:${this.configure.Port}`);

      const ThriftConnection = AUTH_MECHANISMS[this.configure.Auth].connection;
      const option = {
        timeout: this.configure.Timeout,
        transport: AUTH_MECHANISMS[this.configure.Auth].transport,
      };

      const service = TCLIService;
      const serviceType = TCLIServiceTypes;
      const timer = new Timer();

      this.Conn = new ThriftConnection(this.Configure.Host, this.Configure.Port, option);
      this.Client = thrift.createClient(service, this.Conn);

      const request = new serviceType.TOpenSessionReq();

      request.username = this.Configure.Username;
      request.password = this.Configure.Password;

      debug(`Initialize, username -> ${request.username}/ ${request.password}`);

      timer.start();
      debug(`OpenSession request start, ${timer.Start}`, request);

      this.Conn.once('error', (err) => {
        debug('Error caused, from Connection.connect');
        debug(err.message);
        debug(err.stack);

        reject(new ConnectionError(err));
      });

      this.Client.OpenSession(request, (err, res) => {
        timer.end();
        debug(`OpenSession request end -> ${timer.End}/ ${timer.Diff}`, err);

        if (err) {
          reject(new ConnectionError(err));
        } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
          const [errorMessage, infoMessage, message] = HS2Util.getThriftErrorMessage(
            res.status, 'ExecuteStatement operation fail,... !!');

          debug('ExecuteStatement -> Error from HiveServer2\n', infoMessage);
          debug('ExecuteStatement -> Error from HiveServer2\n', errorMessage);

          reject(new ConnectionError(message));
        } else {
          this.SessionHandle = res.sessionHandle;
          this.ThriftConnConfiguration = res.configuration;
          this.ServerProtocolVersion = res.serverProtocolVersion;

          debug('Operation end, OpenSession -> finished');
          debug('Operation end, serverProtocolVersion -> ', this.ServerProtocolVersion);

          const cursor = new HiveCursor(this.Configure, this);
          this.cursor = cursor;
          resolve(cursor);
        }
      });
    });
  }

  close() {
    return new Promise((resolve, reject) => {
      const serviceType = TCLIServiceTypes;
      const timer = new Timer();

      const request = new serviceType.TCloseSessionReq({
        sessionHandle: this.SessionHandle,
      });

      timer.start();
      debug(`Connection close -> CloseSession start, ...${timer.Start}`);

      this.Client.CloseSession(request, (err, res) => {
        timer.end();

        if (err) {
          this.Conn.end();
          reject(new ConnectionError(err));
        } else if (res.status.statusCode === serviceType.TStatusCode.ERROR_STATUS) {
          const [errorMessage, infoMessage, message] = HS2Util.getThriftErrorMessage(
            res.status, 'ExecuteStatement operation fail,... !!');

          debug('ExecuteStatement -> Error from HiveServer2\n', infoMessage);
          debug('ExecuteStatement -> Error from HiveServer2\n', errorMessage);

          this.Conn.end();

          reject(new ConnectionError(message));
        } else {
          this.Conn.end();
          resolve(true);
        }
      });
    });
  }

  async query(sql, processData) {
    if (this.cursor) {
      const execResult = await this.cursor.execute(sql);

      if (execResult.hasResultSet) {
        const schemas = await this.cursor.getSchema();
        const data = await this.cursor.fetchBlock();

        if (processData) {
          return (processData(data.rows, schemas));
        } else {
          return [data, schemas]
        }
      }

    }
  }
}

module.exports = HiveConnection;
