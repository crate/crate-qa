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


function collect(resultSet) {
    let data = [];
    data.colNames = resultSet.fields.map(f => f.name);
    for (let i = 0; i < resultSet.rows.length; i++) {
        let row = resultSet.rows[i];
        let rowData = {};
        for (let j = 0; j < data.colNames.length; j++) {
            let colName = data.colNames[j];
            rowData[colName] = row[colName];
        }
        data.push(rowData);
    }
    return data;
}


const COMMA = ',';


function generateInserts(tableName, data, batchSize) {
    console.log(`Table ${tableName} ${data.length} rows (${Math.ceil(data.length / batchSize)} batches of ~${batchSize})`);
    let colNames = data.colNames.join(',');
    let inserts = [];
    let batchIdx = 0;
    let values = '';
    for (let rowIdx=0; rowIdx < data.length; rowIdx++, batchIdx++) {
        values += '(';
        let colValues = '';
        let row = data.getValues(rowIdx);
        for (let colIdx=0; colIdx < row.length; colIdx++) {
            let colVal = row[colIdx];
            if (isNaN(colVal)) {
                colValues += `'${colVal}'`;
            } else {
                colValues += `${colVal}`;
            }
            colValues += COMMA;
        }
        values += minusDelimiter(colValues);
        values += `)${COMMA}`;
        if (batchIdx >= batchSize) {
            batchIdx = 0;
            inserts.push(generateInsert(tableName, colNames, values));
            values = '';
        }
    }
    if (batchIdx > 0) {
        inserts.push(generateInsert(tableName, colNames, values));
    }
    return inserts;
}


function minusDelimiter(text) {
    return text.substring(0, text.length - COMMA.length);
}


function generateInsert(tableName, colNames, values) {
    return `INSERT INTO ${tableName} (${colNames}) VALUES ${minusDelimiter(values)};`;
}


module.exports = {
    create_pool,
    teardown_pool,
    connect,
    setup_table,
    execute,
    generateInserts,
};
