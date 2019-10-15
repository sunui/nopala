const util = require('util');
const exec = util.promisify(require('child_process').exec);
const fs = require('fs');
const path = require('path');

const IMPALA_REPO = "https://raw.githubusercontent.com/cloudera/Impala/cdh6.3.0"
const HIVE_REPO = "https://raw.githubusercontent.com/apache/hive/rel/release-2.3.3"
const THRIFT_REPO = "https://raw.githubusercontent.com/apache/thrift/master"

const files = [
	`${IMPALA_REPO}/common/thrift/hive-1-api/TCLIService.thrift`,
	`${IMPALA_REPO}/common/thrift/ImpalaService.thrift`,
	`${IMPALA_REPO}/common/thrift/ExecStats.thrift`,
	`${IMPALA_REPO}/common/thrift/Metrics.thrift`,
	`${IMPALA_REPO}/common/thrift/RuntimeProfile.thrift`,
	`${IMPALA_REPO}/common/thrift/Status.thrift`,
	`${IMPALA_REPO}/common/thrift/beeswax.thrift`,
	`${IMPALA_REPO}/common/thrift/Types.thrift`,
	`${IMPALA_REPO}/common/thrift/generate_error_codes.py`,

	`${THRIFT_REPO}/contrib/fb303/if/fb303.thrift`,
	`${HIVE_REPO}/metastore/if/hive_metastore.thrift`
]

const _thrift_gen = path.join(__dirname, './_thrift_gen')
const thrift = path.join(__dirname, './thrift')
const ImpalaService = path.join(__dirname, './thrift/ImpalaService.thrift')
const generate_error_codes = path.join(__dirname, './thrift/generate_error_codes.py')
const ErrorCodes = path.join(__dirname, '../ErrorCodes.thrift')
const hive_metastore = path.join(__dirname, './thrift/hive_metastore.thrift')

Promise.all(files.map(file => {
	return exec(`wget -P ${thrift} ${file} `).then(console.log)
})).then(() => {
	console.log('success download files');
	fs.readFile(hive_metastore, 'utf8', function (err, data) {
		if (err) throw err;
		let newContent = data.replace(/share\/fb303\/if\//g, '');
		fs.writeFile(hive_metastore, newContent, 'utf8', async (err) => {
			if (err) throw err;
			await exec(`python ${generate_error_codes}`)
			console.log('success generate_error_codes');
			await exec(`mv ${ErrorCodes} ${path.join(__dirname, './thrift/ErrorCodes.thrift')}`)
			await exec(`thrift -r --gen js:node -out ${_thrift_gen} ${ImpalaService}`)
			console.log('over~');
		});
	});
})








