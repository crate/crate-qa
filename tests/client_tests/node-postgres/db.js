const uuid = require('uuid/v4');
const Pool = require('pg').Pool;
let pgClientPool


function create_pool(hostname, port) {

    pgClientPool = new Pool({
        user: 'crate',
        password: '',
        host: hostname,
        port: port
    });

}


async function teardown_pool() {
    return pgClientPool.end();
}


async function connect() {
    return pgClientPool.connect();
}


async function setup_table() {
    let id = uuid().substring(0, 16);
    let testTableName = `"doc"."tmp_table_${id}"`;
    await execute(
        `CREATE TABLE ${testTableName} (` +
        '        log_time timestamp,' +
        '        client_ip ip,' +
        '        request string,' +
        '        status_code short,' +
        '        object_size long);'
    )
    return testTableName;
}



function execute(sql, parameters) {
    try {
        return pgClientPool.query(sql, parameters);
    } catch (error) {
        console.error(`The horror: ${error}`);
        throw error;
    }
}



module.exports = {
    create_pool,
    teardown_pool,
    connect,
    setup_table,
    execute,
};
