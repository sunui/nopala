import test from 'ava';
import {createConnection} from '../index.js';

const options = {
	protocol:'hiveserver2',
	resultType: 'json-array',
	auth: 'NOSASL',
	host: '0.0.0.0',
	port: '21050', 
	timeout: 10000,
	nullStr: 'NULL',
	i64ToString: true,
}

const sql=`select * from savior.ui limit 5`

let connection = createConnection(options);

test.before('create a new hiveserver2 connection', async () => {
	await connection.connect();
});

test('hs2 Promise Test Async', async (t) => {
	const execResult = await connection.query(sql);
	t.is(5, execResult.length);
});

test.after('close hiveserver2', async () => {
	// await cursor.close();
	await connection.close();
});


