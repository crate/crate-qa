const cratedb = require('./db');
const test_insert_multivalue = require('./test_insert_multivalue');


async function main(hostname, port) {

    cratedb.create_pool(hostname, port)

    test_insert_multivalue.run()
    .then(() => {
    	console.log("Success")
    })
    .catch((error) => {
    	console.log("Error")
    	throw error
    })
    .finally(() => {
        cratedb.teardown_pool();
    })

}


if (require.main === module) {
    hostname = process.argv.length === 4 ? process.argv[2] : 'localhost'
    port = process.argv.length === 4 ? process.argv[3] : 5432
    main(hostname, port);
}
