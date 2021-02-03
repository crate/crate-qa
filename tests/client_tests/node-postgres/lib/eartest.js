/**
 *
 * eartest: A small custom test framework for `node-postgres`.
 *
 * Etymology:
 * The name is coined after Python's `nosetest` and Lua's `mouthtest`.
 *
 * Features:
 * - Run a number of designated test case files against a `pg.Pool`.
 * - All test cases will be invoked on both the pure JavaScript
 *   driver variant and the native driver variant.
 * - If any single test fails, the `success` attribute will be toggled to `false`.
 *   That can be used to report about the overall outcome of the whole suite.
 *
 * TODO: Introduce a real test framework, see `backlog.rst`.
 *       In the long run, we should use a real testing framework for improved
 *       structure, test case discovery and better reporting. It will probably
 *       also deliver a mechanism for measuring code coverage and other features.
 *
**/


// TODO: Modularize this.
const cratedb = require('./db');

const path = require('path');


class EarTest {

    constructor(hostname, port) {
        this.hostname = hostname;
        this.port = port;
        this.success = true;
    }

    async run(testfiles) {

        for (const use_native of [false, true]) {
            await this.run_files(testfiles, use_native);
        }

    }

    async run_files(testfiles, use_native) {

        const _this = this;

        // Create a database pool handle.
        cratedb.create_pool(this.hostname, this.port, use_native)

        // For signaling to the log output which driver variant has been used.
        let variant_label = `native: ${use_native}`;

        // Run test cases.
        let promises = [];
        for (const testfile of testfiles) {
            const testcase = require(testfile);
            const testname = path.basename(testfile);
            promises.push(new Promise(function(resolve, reject) {
                testcase.run()
                    .then(() => {
                        console.log(`SUCCESS [${variant_label}]: ${testname}`)
                    })
                    .catch((error) => {
                        _this.success = false;
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
}


module.exports = {
    EarTest,
};
