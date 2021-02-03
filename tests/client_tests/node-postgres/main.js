// Add local `lib` folder to list of module search paths.
// https://gist.github.com/branneman/8048520
require.main.paths.push(`${__dirname}/lib`);

const eartest = require('eartest');


// Define test cases. They will be executed asynchronously,
// so please be aware that the current test runner doesn't
// impose any sequential execution constraints.
const testfiles = [
    `${__dirname}/tests/test_insert_multivalue`,
    `${__dirname}/tests/test_parameterized_timestamp`,
];


async function main(hostname, port) {
    const suite = new eartest.EarTest(hostname, port);
    await suite.run(testfiles);
    console.info("Overall success:", suite.success);
    if (!suite.success) {
        process.exit(1);
    }
}


if (require.main === module) {
    hostname = process.argv.length === 4 ? process.argv[2] : 'localhost'
    port = process.argv.length === 4 ? process.argv[3] : 5432
    main(hostname, port);
}
