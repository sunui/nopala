import test from 'ava';
import {createConnection} from '../index.js';

const client = createConnection({
  host: '0.0.0.0',
  port: 21000,
  protocol: 'beeswax',
  resultType: 'json-array'
});

const sql = 'select * from savior.ui limit 10';

test.before('create a new connection', () => {
  client.connect();
});

test('client should have props', (t) => {
  t.is(client.connection.host, '0.0.0.0');
  t.is(client.connection.port, 21000);
  t.is(client.connection.timeout, 1000);
  t.is(client.config.resultType, 'json-array');
});

test('query should return the result', async (t) => {
  const result = await client.query(sql);
  t.is(10, result.length);
  t.truthy(result);
});

test('explain should return the query plan', async (t) => {
  const explanation = await client.connection.explain(sql);
  t.truthy(explanation);
});

test('getResultsMetadata should return the result metadata', async (t) => {
  const metaData = await client.connection.getResultsMetadata(sql);
  t.truthy(metaData);
});

test.after('close the connection', async (t) => {
  const error = await client.close();
  t.truthy(error);
});

