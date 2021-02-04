// TODO: Modularize this.
const cratedb = require.main.require('db');

const chai = require('chai');
chai.use(require('chai-datetime'));

const assert = chai.assert;


async function run() {
    /*
    Investigate parameterized INSERT statements with TIMESTAMP columns
    (server-side binding) using CrateDB with PostgreSQL protocol.

    https://gist.github.com/amotl/b25297e7dc5a7f744aebb04ce4960833
    */

    // Set up a ephemeral database table.
    let testTableName = await cratedb.setup_table();

    // Invoke the `INSERT` statement.
    await cratedb.execute(
        `INSERT INTO ${testTableName} ("log_time") VALUES ($1);`,
        ['2021-01-13T14:37:17.25988Z']
    );

    // Make sure the `INSERT` is synchronized.
    await cratedb.execute(`REFRESH TABLE ${testTableName};`);

    // Check the outcome.
    let response = await cratedb.execute(`SELECT * FROM ${testTableName};`);
    let data = response.rows;
    if (!data) {
        throw new Error('expected data, got nothing back');
    }
    assert.lengthOf(data, 1);
    assert.property(data[0], 'log_time');
    assert.typeOf(data[0]['log_time'], 'Date');
    assert.equalDate(data[0]['log_time'], new Date('2021-01-13T14:37:17.25988Z'));

}


module.exports = {
    run,
};
