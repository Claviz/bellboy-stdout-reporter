import { Job, Reporter } from 'bellboy';
import chalk from 'chalk';
import filesize from 'filesize';
import prettyMs from 'pretty-ms';

import { IReporterConfig, IStream, LogItem } from './types';
import { getCurrentTimestamp, stringify } from './utils';

let globalJobId = 0;

export default class StdoutReporter extends Reporter {

    private interval: number;
    private verbose: boolean;

    constructor(config?: IReporterConfig) {
        super();
        this.interval = 5000;
        this.verbose = false;
        if (config) {
            this.interval = config.interval || 5000;
            this.verbose = config.verbose || false;
        }
    }

    report(job: Job) {
        let jobId = globalJobId.toString();
        if ((job as any).jobName) {
            jobId += ` (${(job as any).jobName})`;
        }
        globalJobId++;
        let stream: IStream;
        const startedMs = (new Date()).getTime();

        let timeoutObj: NodeJS.Timeout | null;

        function logItem(item: LogItem, indent: boolean = true, withTimestamp: boolean = true) {
            if (indent) {
                console.log();
            }
            let header = item.header;
            if (item.type === 'success') {
                header = `${chalk.green('\u221A')} ${header}`;
            } else if (item.type === 'fail') {
                header = `${chalk.red('x')} ${header}`;
            } else if (item.type === 'wait') {
                header = `${chalk.yellow('⌛︎')} ${header}`;
            }
            console.log(chalk.bold(header));
            if (withTimestamp) {
                item.lines = {
                    'Timestamp': getCurrentTimestamp(),
                    ...item.lines,
                }
            }
            if (item.lines || item.children) {
                console.group();
                if (item.lines) {
                    const lineKeys = Object.keys(item.lines);
                    for (let i = 0; i < lineKeys.length; i++) {
                        const key = lineKeys[i];
                        const value = item.lines[key];
                        console.log(`${chalk.bold(key)}: ${value}`);
                    }
                }
                if (item.children) {
                    for (let i = 0; i < item.children.length; i++) {
                        logItem(item.children[i], false, false);
                    }
                }
                console.groupEnd();
            }
        }

        function getStreamStatus() {
            let header = `Job #${jobId}. Stream #${stream.streamId} `;
            header += stream.streamInfo && stream.streamInfo.length ? `(${stream.streamInfo.join(', ')}) ` : '';
            if (stream.finished) {
                header += 'processed';
            } else {
                const speed = stream.speed ? ` (${Math.round(stream.speed)} rows/sec)` : '';
                header += `processing${speed}…`;
            }
            const streamLogItem: LogItem = {
                type: stream.finished ? 'success' : 'wait',
                header,
                lines: {
                    'Rows': chalk.green(`${stream.receivedRows} received (${filesize(stream.bytes)})`)
                },
                children: [],
            };
            const destinationIds = Object.keys(stream.destinations);
            for (let i = 0; i < destinationIds.length; i++) {
                const destinationId = destinationIds[i];
                const destination = stream.destinations[Number(destinationId)];
                const destinationLogItem: LogItem = {
                    header: `Destination #${destinationId}`,
                    lines: {},
                }
                if (destination.rowsGenerated || destination.rowsGenerationFails) {
                    const arr = [];
                    if (destination.rowsGenerated) {
                        arr.push(chalk.green(`${destination.rowsGenerated} generated`));
                    }
                    if (destination.rowsGenerationFails) {
                        arr.push(chalk.red(`${destination.rowsGenerationFails} failed`));
                    }
                    destinationLogItem.lines!['Row generation'] = arr.join(', ');
                }
                if (destination.batchesTransformed || destination.batchTransformFails) {
                    const arr = [];
                    if (destination.batchesTransformed) {
                        arr.push(chalk.green(`${destination.batchesTransformed} transformed`));
                    }
                    if (destination.batchTransformFails) {
                        arr.push(chalk.red(`${destination.batchTransformFails} failed`));
                    }
                    destinationLogItem.lines!['Row transformation'] = arr.join(', ');
                }
                if (destination.batchesLoaded || destination.batchLoadFails) {
                    const arr = [];
                    if (destination.batchesLoaded) {
                        arr.push(chalk.green(`${destination.batchesLoaded} loaded(${filesize(destination.bytesLoaded)})`));
                    }
                    if (destination.batchLoadFails) {
                        arr.push(chalk.red(`${destination.batchLoadFails} failed`));
                    }
                    destinationLogItem.lines!['Batch load'] = arr.join(', ');
                }
                streamLogItem.children!.push(destinationLogItem);
            }
            logItem(streamLogItem);
        }

        function getDestination(destinationIndex: number) {
            let destination = stream.destinations[destinationIndex];
            if (!destination) {
                destination = {
                    batchLoadFails: 0,
                    batchesLoaded: 0,
                    rowsGenerated: 0,
                    rowsGenerationFails: 0,
                    batchTransformFails: 0,
                    batchesTransformed: 0,
                    bytesLoaded: 0,
                };
                stream.destinations[destinationIndex] = destination;
            }
            return destination;
        }

        job.on('startProcessingRow', async (data) => {
            stream.receivedRows++;
            stream.bytes += Buffer.byteLength(JSON.stringify(data), 'utf8');
            if (this.verbose) {
                logItem({
                    type: 'success',
                    header: `Job #${jobId}. Stream #${stream.streamId}. Row received`,
                    lines: {
                        'Data': stringify(data),
                    }
                });
            }
        });
        job.on('rowGenerated', async (destinationIndex, data) => {
            const destination = getDestination(destinationIndex);
            destination.rowsGenerated++;
            if (this.verbose) {
                logItem({
                    type: 'success',
                    header: `Job #${jobId}. Stream #${stream.streamId}. Destination #${destinationIndex} Row generated`,
                    lines: {
                        'Data': stringify(data),
                    }
                });
            }
        });
        job.on('rowGenerationError', async (destinationIndex, data, error) => {
            const destination = getDestination(destinationIndex);
            destination.rowsGenerationFails++;
            logItem({
                type: 'fail',
                header: `Job #${jobId}. Stream #${stream.streamId}. Destination #${destinationIndex}. Row generation failed`,
                lines: {
                    'Source row': stringify(data),
                    'Error': error,
                }
            });
        });
        job.on('transformingBatchError', async (destinationIndex, data, error) => {
            const destination = getDestination(destinationIndex);
            destination.batchTransformFails++;
            logItem({
                type: 'fail',
                header: `Job #${jobId}. Stream #${stream.streamId}. Destination #${destinationIndex}. Batch transform failed`,
                lines: {
                    'Source data': stringify(data),
                    'Error': error,
                }
            });
        });
        job.on('transformedBatch', async (destinationIndex, data) => {
            const destination = getDestination(destinationIndex);
            destination.batchesTransformed++;
            if (this.verbose) {
                logItem({
                    type: 'success',
                    header: `Job #${jobId}. Stream #${stream.streamId}. Destination #${destinationIndex} Batch transformed`,
                    lines: {
                        'Data': stringify(data),
                    }
                });
            }
        });
        job.on('loadingBatchError', async (destinationIndex, data, error) => {
            const destination = getDestination(destinationIndex);
            destination.batchLoadFails++;
            logItem({
                type: 'fail',
                header: `Job #${jobId}. Stream #${stream.streamId}. Destination #${destinationIndex}. Batch load failed`,
                lines: {
                    'Source data': stringify(data),
                    'Error': error,
                }
            });
        });
        job.on('loadedBatch', async (destinationIndex, data) => {
            const destination = getDestination(destinationIndex);
            destination.batchesLoaded++;
            destination.bytesLoaded += Buffer.byteLength(JSON.stringify(data), 'utf8');
            if (this.verbose) {
                logItem({
                    type: 'success',
                    header: `Job #${jobId}. Stream #${stream.streamId}. Destination #${destinationIndex} Batch loaded`,
                    lines: {
                        'Data': stringify(data),
                    }
                });
            }
        });
        job.on('startProcessing', async () => {
            logItem({
                type: 'success',
                header: `Job #${jobId} started`,
            });
        });
        job.on('endProcessing', async () => {
            const finishedMs = (new Date()).getTime();
            const durationMs = finishedMs - startedMs;
            logItem({
                type: 'success',
                header: `Job #${jobId} completed`,
                lines: {
                    'Job duration': prettyMs(durationMs),
                }
            });
        });
        job.on('processingError', async (err) => {
            logItem({
                type: 'fail',
                header: `Job #${jobId} failed`,
                lines: {
                    'Error': err,
                }
            });
        });
        let timePassed = 0;
        job.on('startProcessingStream', async (...args) => {
            stream = {
                receivedRows: 0,
                destinations: [],
                bytes: 0,
                streamId: stream && stream.streamId ? stream.streamId : 0,
                streamInfo: args,
                finished: false,
                speed: 0,
            };
            timeoutObj = setInterval(() => {
                if (!stream.finished) {
                    timePassed += this.interval;
                    const speed = stream.receivedRows / (timePassed / 1000);
                    stream.speed = speed;
                    getStreamStatus();
                }
            }, this.interval);
        });
        job.on('endProcessingStream', async (...argumets) => {
            if (timeoutObj) {
                clearInterval(timeoutObj);
                timeoutObj = null;
            }
            stream.finished = true;
            getStreamStatus();
            stream.streamId++;
        });
    }
}