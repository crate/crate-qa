const cratedb = require('./db');


async function main(hostname, port) {

    // TODO: Introduce a real test framework, see `backlog.rst`.

    for (const use_native of [false, true]) {
        await suite(hostname, port, use_native);
    }

}

async function suite(hostname, port, use_native) {

    // For signaling which driver variant has been used.
    let variant_label = `native: ${use_native}`;

    // Create a database pool handle.
    cratedb.create_pool(hostname, port, use_native)

    // Define test cases. They will be executed in parallel
    // as the current structure doesn't impose any sequential
    // execution constraints.
    let testnames = [
        'test_insert_multivalue',
        'test_parameterized_timestamp',
    ];

    // Run test cases.
    let promises = [];
    for (const testname of testnames) {
        const testcase = require(`./${testname}`);
        promises.push(new Promise(function(resolve, reject) {
            testcase.run()
                .then(() => {
                    console.log(`SUCCESS [${variant_label}]: ${testname}`)
                })
                .catch((error) => {
                    console.log(`ERROR   [${variant_label}]: ${testname}`)
                    console.trace(error);
                })
                .finally(() => {
                    resolve();
                })
        }));
    }

    // This synchronizes all test steps and will only tear down
    // the client pool after all steps have finished.
    return Promise.all(promises).finally(async() => {
        await cratedb.teardown_pool();
    });

}


if (require.main === module) {
    hostname = process.argv.length === 4 ? process.argv[2] : 'localhost'
    port = process.argv.length === 4 ? process.argv[3] : 5432
    main(hostname, port);
}
