const cratedb = require('./db');
const test_csv_import = require('./test_csv_import');


async function main(hostname, port) {

    cratedb.create_pool(hostname, port)

    test_csv_import.run().finally(() => {
        cratedb.teardown_pool();
    })

}


if (require.main === module) {
    hostname = process.argv.length === 4 ? process.argv[2] : 'localhost'
    port = process.argv.length === 4 ? process.argv[3] : 5432
    main(hostname, port);
}
