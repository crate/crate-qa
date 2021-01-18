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
        '        log_time timestamp NOT NULL,' +
        '        client_ip ip NOT NULL,' +
        '        request string NOT NULL,' +
        '        status_code short NOT NULL,' +
        '        object_size long NOT NULL);'
    )
    return testTableName;
}



function execute(sql) {
    try {
        return pgClientPool.query(sql);
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
