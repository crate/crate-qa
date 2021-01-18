const fs = require('fs').promises
const csv = require('async-csv');
const expect = require('chai').expect;
const path = require('path');

const cratedb = require('./db');


const dataFile = path.resolve(path.dirname(require.main.filename), 'resources/log_entries.csv');
const batchSize = 10;
const expectedRowCount = 10;        // wc -l log_entries.csv minus one for the header
const expectedUpdatedRowCount = 4;  // grep ",0$" log_entries.csv | wc -l


async function run() {

    const data = await load_csv(dataFile);

    let testTableName = await cratedb.setup_table();

    let startLoadTs = Date.now();
    let inserts = cratedb.generateInserts(testTableName, data, batchSize);
    for (let i=0; i < inserts.length; i++) {
        await cratedb.execute(inserts[i]);
    }
    await cratedb.execute(`REFRESH TABLE ${testTableName};`);

    return check_data(testTableName)

}


async function load_csv(filePath, sep, callback) {

    const csvString = await fs.readFile(filePath, "utf-8");
    const results = await csv.parse(csvString, { columns: true });

    if (results) {
        let colNames = getkeys(results[0]);
        results['colNames'] = colNames;
        results['getValues'] = (i) => {
            let values = [];
            let row = results[i];
            for (let j=0; j < colNames.length; j++) {
                let cn = colNames[j]
                values.push(row[cn]);
            }
            return values;
        };
    }

    return results;

}


function getkeys(obj) {
    let keys = [];
    for (let k in obj) {
        if (obj.hasOwnProperty(k)) {
            keys.push(k);
        }
    }
    return keys;
}


async function check_data(testTableName) {

    let response
    let data

    response = await cratedb.execute(`SELECT * FROM ${testTableName};`)
    data = response.rows
    if (!data) {
        throw new Error('expected data, got nothing back');
    }
    expect(data).to.have.lengthOf(expectedRowCount);
    let i = Math.floor(Math.random() * expectedRowCount);
    expect(data[i]).to.have.property('log_time');
    expect(data[i]).to.have.property('client_ip');
    expect(data[i]).to.have.property('request');
    expect(data[i]).to.have.property('status_code');
    expect(data[i]).to.have.property('object_size');

    response = await cratedb.execute(`update ${testTableName} set object_size = 40 where object_size=0 returning *;`)
    data = response.rows
    if (!data) {
        throw new Error('expected data, got nothing back');
    }
    let updateCount = data.filter((row) => row['object_size'] == 40);
    expect(updateCount).to.have.lengthOf(expectedUpdatedRowCount);
    let zeroCount = data.filter((row) => row['object_size'] == 0);
    expect(zeroCount).to.have.lengthOf(0);
    await cratedb.execute(`DROP TABLE ${testTableName};`)
}


module.exports = {
    run,
};
