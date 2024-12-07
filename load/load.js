// Import the http module to make HTTP requests. From this point, you can use `http` methods to make HTTP requests.
import http from 'k6/http';
import exec from 'k6/execution';
import {check} from 'k6';

// Import the sleep function to introduce delays. From this point, you can use the `sleep` function to introduce delays in your test script.
import {sleep} from 'k6';

const NUMBER_OF_TABLES = 1500;
const NUMBER_OF_WORKERS = 1;

export const options = {
    // Key configurations for Stress in this section
    stages: [
        {duration: '1s', target: NUMBER_OF_WORKERS}, // traffic ramp-up from 1 to a higher 200 users over 10 minutes.
        {duration: '2m', target: NUMBER_OF_WORKERS}, // stay at higher 200 users for 30 minutes
        {duration: '1m', target: 0}, // ramp-down to 0 users
    ],
};

/**
 * Returns a random number between min (inclusive) and max (exclusive)
 */
function getRandomArbitrary(min, max) {
    return Math.random() * (max - min) + min;
}

/**
 * Returns a random integer between min (inclusive) and max (inclusive).
 * The value is no lower than min (or the next integer greater than min
 * if min isn't an integer) and no greater than max (or the next integer
 * lower than max if max isn't an integer).
 * Using Math.round() will give you a non-uniform distribution!
 */
function getRandomInt(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min + 1)) + min;
}


export default function () {
    // Make a GET request to the target URL

    let per_worker = Math.floor(NUMBER_OF_TABLES / NUMBER_OF_WORKERS)
    let slice = exec.vu.idInTest * per_worker
    let number_between_0_and_1500 = Math.min(getRandomInt(slice, Math.min(slice + per_worker, NUMBER_OF_TABLES - 1)), NUMBER_OF_TABLES - 1);

    const c = http.get('http://localhost:8181/catalog/v1/a89475d6-b426-11ef-a817-9bb0eaa896e9/namespaces/demo_namespace/tables/my_table_' + number_between_0_and_1500);

    let r = JSON.parse(c.body);
    let tab = r['metadata']
    let ts;
    if (c.status !== 200) {
        console.log("FAILED GET: ", number_between_0_and_1500, c.status, c.body)
    }
    if (tab['snapshot-log'] === undefined) {
        tab['snapshot-log'] = [];
    }
    if (tab['snapshot-log'].length > 0) {
        ts = tab['snapshot-log'][tab['snapshot-log'].length - 1]['timestamp-ms'];
    } else {
        ts = 0;
    }
    if (ts === undefined) {
        ts = 0;
    }
    // parse timestamp ms to number

    // get max_sequence_number from snapshots
    let snaps = tab['snapshots'].map(s => s['sequence-number']);
    let seqn = Math.max(...snaps);
    if (seqn === -Infinity) {
        seqn = 0;
    }
    // let snap_id = Math.max(...tab['snapshots'].map(s => s['snapshot-id']));
    // if (snap_id === undefined) {
    //     snap_id = 0;
    // }
    let snap_id = Math.random() * 9_223_372_036_854_775_807
    const payload = JSON.stringify({

        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": snap_id,
                    "sequence-number": seqn + 1,
                    "timestamp-ms": Date.now(),
                    "manifest-list": "s3://examples/initial-warehouse/019398f0-d388-7d01-ae57-7a80dd5864e1/019398f1-1343-7681-a1d7-aba8ff078103/metadata/snap-2132429083345208514-1-08de7159-efef-4dbc-b7eb-55ee7f995f66.avro",
                    "summary": {
                        "operation": "append",
                        "total-position-deletes": "0",
                        "total-delete-files": "0",
                        "added-records": "1",
                        "added-data-files": "1",
                        "total-data-files": "3",
                        "iceberg-version": "Apache Iceberg 1.6.1 (commit 8e9d59d299be42b0bca9461457cd1e95dbaad086)",
                        "total-records": "3",
                        "engine-name": "spark",
                        "added-files-size": "392284",
                        "changed-partition-count": "1",
                        "total-equality-deletes": "0",
                        "spark.app.id": "local-1733438058307",
                        "total-files-size": "1176868",
                        "engine-version": "3.5.1",
                        "app-id": "local-1733438058307"
                    },
                    "schema-id": 0
                }
            }
        ]
    })
    const headers = {"Content-Type": "application/json"};
    let res = http.post('http://localhost:8181/catalog/v1/a89475d6-b426-11ef-a817-9bb0eaa896e9/namespaces/demo_namespace/tables/my_table_' + number_between_0_and_1500,
        payload,
        {headers: headers});

    if (res.status !== 200) {
        console.log("Failed: ", number_between_0_and_1500, res.status, "body: ", res.body)
        console.log("tab", tab);
        console.log("payload: ", payload)
    }
    check(res, {
        'is status 200': (r) => r.status === 200,
    });
    // Sleep for 1 second to simulate real-world usage
    sleep(1);
}


