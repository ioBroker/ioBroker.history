import path from 'node:path';
import assert from 'node:assert';
import { tests } from '@iobroker/testing';
import { preInit, register } from './lib/testCases';

const adapterShortName = 'history';

/*
 * Integration tests based on @iobroker/testing.
 *
 * A fresh JS-Controller environment is spun up for each suite. The two suites run in order and
 * share the on-disk history files (which live in `iobroker-data/history/` and are not wiped
 * between suites), so the second "existing" suite sees the data written by the first one -
 * this is what `assumeExistingData` in the shared test cases expects.
 */
tests.integration(path.join(__dirname, '..'), {
    defineAdditionalTests({ suite, it }) {
        // Scenario 1: writeNulls enabled, starting without any pre-existing history data
        suite(`Test ${adapterShortName}-writeNulls adapter`, getHarness => {
            let harness: ReturnType<typeof getHarness>;
            const sendTo = (
                target: string,
                command: string,
                message: any,
                callback: (result: any) => void,
            ): void => harness.sendTo(target, command, message, callback);

            before(async function () {
                this.timeout(600000);
                harness = getHarness();

                await harness.changeAdapterConfig(adapterShortName, {
                    native: {
                        writeNulls: true,
                        enableDebugLogs: true,
                    },
                });

                // Create the test data points with their custom history config before the adapter starts
                await preInit(harness.objects, harness.states, adapterShortName);

                await harness.startAdapterAndWait();
            });

            it(`Test ${adapterShortName}-writeNulls adapter: Check if adapter started`, function () {
                this.timeout(60000);
                assert.ok(harness.isAdapterRunning(), 'The adapter is not running');
            });

            register(it, sendTo, adapterShortName, true, 0, 0);
        });

        // Scenario 2: writeNulls disabled, building on the data written by scenario 1
        suite(`Test ${adapterShortName}-existing adapter`, getHarness => {
            let harness: ReturnType<typeof getHarness>;
            const sendTo = (
                target: string,
                command: string,
                message: any,
                callback: (result: any) => void,
            ): void => harness.sendTo(target, command, message, callback);

            before(async function () {
                this.timeout(600000);
                harness = getHarness();

                await harness.changeAdapterConfig(adapterShortName, {
                    native: {
                        writeNulls: false,
                        enableDebugLogs: true,
                    },
                });

                await preInit(harness.objects, harness.states, adapterShortName);

                await harness.startAdapterAndWait();
            });

            it(`Test ${adapterShortName}-existing adapter: Check if adapter started`, function () {
                this.timeout(60000);
                assert.ok(harness.isAdapterRunning(), 'The adapter is not running');
            });

            register(it, sendTo, adapterShortName, false, 1, 0);
        });
    },
});
