import { DynamicProcessor, Job } from 'bellboy';
import stripAnsi from 'strip-ansi';

import StdoutReporter from '../src';
import {
    DummyDestination,
    FaultyDestination,
    FaultyProcessor,
    MultipleStreamProcessor,
    timeout,
    WithInfoProcessor,
} from './helpers';

jest.mock('pretty-ms', () => (() => 'pretty-ms-mock'));
jest.mock('../src/utils', () => ({
    ...jest.requireActual('../src/utils'),
    getCurrentTimestamp: () => 'timestamp-mock'
}));

let consoleData: string[] = [];

beforeAll(async () => {
});

beforeEach(async () => {
    consoleData = [];
    let indent = 0;
    console.log = function (message: string) {
        message = message ? message : '';
        consoleData.push('    '.repeat(indent) + stripAnsi(message));
    }
    console.group = function () {
        indent++;
    }
    console.groupEnd = function () {
        indent--;
    }
});

afterEach(async () => {
});

afterAll(async () => {
});

it('logs successfull bellboy job', async () => {
    const destination = new DummyDestination();
    const processor = new DynamicProcessor({
        generator: async function* () {
            yield `test`;
        },
    });
    const job = new Job(processor, [destination], {
        reporters: [new StdoutReporter()],
    });
    await job.run();
    expect(consoleData).toMatchSnapshot();
});

it('logs load exceptions', async () => {
    const destination = new FaultyDestination();
    const processor = new DynamicProcessor({
        generator: async function* () {
            yield `test`;
        },
    });
    const job = new Job(processor, [destination], {
        reporters: [new StdoutReporter()],
    });
    await job.run();
    expect(consoleData).toMatchSnapshot();
});

it('logs intermediate results after reaching some interval', async () => {
    const destination = new DummyDestination();
    const processor = new DynamicProcessor({
        generator: async function* () {
            yield `test`;
            await timeout(1000);
        },
    });
    const job = new Job(processor, [destination], {
        reporters: [new StdoutReporter({ interval: 1000 })],
    });
    await job.run();
    expect(consoleData).toMatchSnapshot();
});

it('logs failing bellboy job', async () => {
    const destination = new DummyDestination();
    const processor = new FaultyProcessor();
    const job = new Job(processor, [destination], {
        reporters: [new StdoutReporter()],
    });
    await job.run();
    expect(consoleData).toMatchSnapshot();
});

it('logs destination batch transformations', async () => {
    const destination = new DummyDestination({
        batchTransformer: async function (rows) {
            if (rows[0] === `shouldSuccess`) {
                return rows;
            } else {
                throw new Error(`Oh, snap!`);
            }
        },
        batchSize: 1,
    });
    const processor = new DynamicProcessor({
        generator: async function* () {
            yield `shouldSuccess`;
            yield `shouldFail`;
        },
    });
    const job = new Job(processor, [destination], {
        reporters: [new StdoutReporter()],
    });
    await job.run();
    expect(consoleData).toMatchSnapshot();
});

it('logs destination record generations', async () => {
    const destination = new DummyDestination({
        recordGenerator: async function* (row) {
            if (row === `shouldSuccess`) {
                yield row;
            } else {
                throw new Error(`Oh, snap!`);
            }
        },
    });
    const processor = new DynamicProcessor({
        generator: async function* () {
            yield `shouldSuccess`;
            yield `shouldFail`;
        },
    });
    const job = new Job(processor, [destination], {
        reporters: [new StdoutReporter()],
    });
    await job.run();
    expect(consoleData).toMatchSnapshot();
});

it('logs stream info if stream emits one', async () => {
    const destination = new DummyDestination();
    const processor = new WithInfoProcessor();
    const job = new Job(processor, [destination], {
        reporters: [new StdoutReporter()],
    });
    await job.run();
    expect(consoleData).toMatchSnapshot();
});

it('logs multiple streams', async () => {
    const destination = new DummyDestination();
    const processor = new MultipleStreamProcessor();
    const job = new Job(processor, [destination], {
        reporters: [new StdoutReporter()],
    });
    await job.run();
    expect(consoleData).toMatchSnapshot();
});

it('logs successfull bellboy job in verbose mode', async () => {
    const destination = new DummyDestination({
        batchTransformer: async function (rows) {
            return rows;
        },
        recordGenerator: async function* (row) {
            yield row;
        }
    });
    const processor = new DynamicProcessor({
        generator: async function* () {
            yield `test`;
        },
    });
    const job = new Job(processor, [destination], {
        reporters: [new StdoutReporter({ verbose: true })],
    });
    await job.run();
    expect(consoleData).toMatchSnapshot();
});

it('logs truncated array', async () => {
    const destination = new DummyDestination();
    const processor = new DynamicProcessor({
        generator: async function* () {
            for (let i = 0; i < 10; i++) {
                yield `test`;
            }
        },
    });
    const job = new Job(processor, [destination], {
        reporters: [new StdoutReporter({ verbose: true })],
    });
    await job.run();
    expect(consoleData).toMatchSnapshot();
});

it('logs bellboy job with custom name', async () => {
    const destination = new DummyDestination();
    const processor = new DynamicProcessor({
        generator: async function* () {
            yield `test`;
        },
    });
    const job = new Job(processor, [destination], {
        jobName: 'Funny job name',
        reporters: [new StdoutReporter()],
    });
    await job.run();
    expect(consoleData).toMatchSnapshot();
});