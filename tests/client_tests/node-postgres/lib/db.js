const uuid = require('uuid/v4');
let pgClientPool


function create_pool(hostname, port, use_native=false) {

    let pg;
    if (!use_native) {
        pg = require('pg');
    } else {
        pg = require('pg').native;
    }

    const Pool = pg.Pool;

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
        '        log_time TIMESTAMP,' +
        '        client_ip IP,' +
        '        request STRING,' +
        '        status_code SHORT,' +
        '        object_size LONG)'
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