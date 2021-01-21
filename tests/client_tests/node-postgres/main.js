const cratedb = require('./db');


async function main(hostname, port) {

    // TODO: Introduce a real test framework, see `backlog.rst`.

    cratedb.create_pool(hostname, port)

    // Define test cases. They will be executed in parallel
    // as the current structure doesn't impose any sequential
    // execution constraints.
    let testnames = [
        'test_insert_multivalue',
        'test_parameterized_timestamp',
    ];

    let promises = [];
    for (const testname of testnames) {
        const testcase = require(`./${testname}`);
        promises.push(new Promise(function(resolve, reject) {
            testcase.run()
                .then(() => {
                    console.log(`SUCCESS: ${testname}`)
                })
                .catch((error) => {
                    console.log(`ERROR:   ${testname}`)
                    throw error
                })
                .finally(() => {
                    resolve();
                })
        }));
    }

    // This synchronizes all test steps and will only tear down
    // the client pool after all steps have finished.
    Promise.all(promises).finally(() => {
        cratedb.teardown_pool();
    });

}


if (require.main === module) {
    hostname = process.argv.length === 4 ? process.argv[2] : 'localhost'
    port = process.argv.length === 4 ? process.argv[3] : 5432
    main(hostname, port);
}
