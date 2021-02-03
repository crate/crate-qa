const cratedb = require('./db');

const expect = require('chai').expect;

const expectedRowCount = 10;        // wc -l log_entries.csv minus one for the header
const expectedUpdatedRowCount = 4;  // grep ",0$" log_entries.csv | wc -l


async function run() {

    // Set up a ephemeral database table.
    let testTableName = await cratedb.setup_table();

    // Invoke the `INSERT` statement.
    await cratedb.execute(get_insert(testTableName));

    // Make sure the `INSERT` is synchronized.
    await cratedb.execute(`REFRESH TABLE ${testTableName};`);

    // Check the outcome.
    return check_data(testTableName)

}

function get_insert(tablename) {

    return `
        INSERT INTO ${tablename}
            (log_time,client_ip,request,status_code,object_size)
            VALUES
            ('2012-01-01T00:00:00Z','25.152.171.147','/books/Six_Easy_Pieces.html',404,271),
            ('2012-01-01T00:00:03Z','243.180.100.114','/slideshow/1.jpg',304,0),
            ('2012-01-01T00:00:03Z','149.60.38.76','/courses/cs100/finalprojects/adventure/javadocs_/index.html?index-filesindex-16.html',200,705),
            ('2012-01-01T00:00:10Z','243.180.100.114','/slideshow/2.jpg',304,0),
            ('2012-01-01T00:00:11Z','134.121.15.97','/courses/cs101/old/2002/syllabus.html',404,277),
            ('2012-01-01T00:00:17Z','243.180.100.114','/slideshow/3.jpg',304,0),
            ('2012-01-01T00:00:17Z','252.202.20.160','/degrees/masters/',200,3233),
            ('2012-01-01T00:00:17Z','252.202.20.160','/degrees/masters/masters.gif',200,7921),
            ('2012-01-01T00:00:18Z','149.60.38.76','/people/wvv/marron/',200,1642),
            ('2012-01-01T00:00:18Z','134.121.15.97','/about/rooms/345/',304,0);
    `

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
