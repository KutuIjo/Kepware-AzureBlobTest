const {
  StorageSharedKeyCredential,
  BlobServiceClient
  } = require('@azure/storage-blob');
const {AbortController} = require('@azure/abort-controller');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const mqtt_data = require('./MQTTresult.json');

if (process.env.NODE_ENV !== "production") {
  require("dotenv").config();
}

const STORAGE_ACCOUNT_NAME = process.env.AZURE_STORAGE_ACCOUNT_NAME;
const ACCOUNT_ACCESS_KEY = process.env.AZURE_STORAGE_ACCOUNT_ACCESS_KEY;
const CONTAINER_NAME = process.env.AZURE_STORAGE_CONTAINER_NAME;

const ONE_MINUTE = 60 * 1000;
const arrAvg = arr => arr.reduce((a,b) => a + b, 0) / arr.length
const arrMax = arr => Math.max(...arr);
const arrMin = arr => Math.min(...arr);
const getMinTimestamp = (data1,data2) => Math.max(data1[0].t, data2[0].t);
const getMaxTimestamp = (data1,data2) => Math.min(data1[data1.length-1].t, data2[data2.length-1].t);

function combineValues(data) {
  var outputarr = [];
  function pushValue(item) {
    outputarr = outputarr.concat(item.values);
  }
  data.forEach(pushValue);
  return outputarr;
};

function sliceData(data,minparam,maxparam) {
  minindex = data.findIndex(element => element.t >= minparam);
  maxindex = data.findIndex(element => element.t >= maxparam) + 1;
  return data.slice(minindex,maxindex);
};

function calcDataConsistency(data_test,data_ideal,tags) { //data 1 azure, data 2 mqtt
  var error_count = 0;
  var all_count = data_ideal.length;
  for (tag of tags) {
    filtered_data_test = data_test.filter(element => element.id === tag);
    filtered_data_ideal = data_ideal.filter(element => element.id === tag);
    for (i = 0; i < filtered_data_test.length; i++){
      var index = -1;
      index = filtered_data_ideal.findIndex(x => x.value === filtered_data_test[i].value);
      if (index > -1) {
        filtered_data_ideal.splice(index, 1);
      }
    }
    error_count += filtered_data_ideal.length;
    // console.log(filtered_data_ideal.length);
  }
  return [error_count,all_count,(all_count-error_count)/all_count];
};

String.prototype.splice = function(idx, rem, str) {
  return this.slice(0, idx) + str + this.slice(idx + Math.abs(rem));
};

async function showContainerNames(aborter, blobServiceClient) {
  let iter = await blobServiceClient.listContainers(aborter);
  for await (const container of iter) {
    console.log(` - ${container.name}`);
  }
}

async function showBlobNames(aborter, containerClient) {
  let iter = await containerClient.listBlobsFlat(aborter);
  for await (const blob of iter) {
    console.log(` - ${blob.name}\n`);
  }
}

async function getBlobName(aborter, containerClient) {
  let iter = await containerClient.listBlobsFlat(aborter);
  for await (const blob of iter) {
    var result = blob.name;
  }
  return await result;
}

// [Node.js only] A helper method used to read a Node.js readable stream into string
async function streamToString(readableStream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readableStream.on("data", (data) => {
      chunks.push(data.toString());
    });
    readableStream.on("end", () => {
      resolve(chunks.join(""));
    });
    readableStream.on("error", reject);
  });
}

// Calculate Delta Time
function calculateDeltaGatewaytoHub(dataPoint) {
  var delta = Date.parse(dataPoint.IoTHub.EnqueuedTime) - dataPoint.timestamp;
  return delta;
}

// Create Summary
function calculateSummaryData(name,arr) {
  var statdict = {
    name: name,
    max: arrMax(arr),
    min: arrMin(arr),
    avg: arrAvg(arr)
  }
  return statdict;
}

function calculateSummary(data) {
  var GatewaytoHubArr = [];
  var BufferSizeArr = [];
  var TagsErrorCount = [];
  var TagsMQTTCount = [];
  var DataConsistency = [];
  var calculation = [];

  for (var i = 0; i < data.length; i++) {
    GatewaytoHubArr.push(calculateDeltaGatewaytoHub(data[i]));
    BufferSizeArr.push((data[i].values).length);
}

// Calculate data consistency
azure_values_combined = combineValues(data);
mqtt_values_combined = combineValues(mqtt_data);

var minTimestamp = getMinTimestamp(mqtt_values_combined,azure_values_combined);
var maxTimestamp = getMaxTimestamp(mqtt_values_combined,azure_values_combined);

// Slice based on min max
var azure_values_sliced = sliceData(azure_values_combined,minTimestamp,maxTimestamp);
var mqtt_values_sliced = sliceData(mqtt_values_combined,minTimestamp,maxTimestamp);

// Get tags available
const Tags = [...new Set(azure_values_sliced.map(item => item.id))];

// Calculate by for each -> filter by Tags -> find azure values di mqtt values
var dataConsistencyParam = calcDataConsistency(azure_values_sliced,mqtt_values_sliced,Tags);
TagsErrorCount.push(dataConsistencyParam[0]);
TagsMQTTCount.push(dataConsistencyParam[1]);
DataConsistency.push(dataConsistencyParam[2]);

var Items = ["IoT Gateway to IoT Hub Latency", "Buffer size", "Error Tags Count", "Sent Tags Count" , "Data Consistency"];
var ItemsData = [GatewaytoHubArr, BufferSizeArr,TagsErrorCount,TagsMQTTCount, DataConsistency];
for (var i = 0; i < ItemsData.length; i++) {
  calculation.push(calculateSummaryData(Items[i],ItemsData[i]));
}

return(calculation);
}

function export_summarycsv(data) {
  const csvWriter = createCsvWriter({
    path: 'Summary.csv',
    header: [
      {id: 'name', title: 'Name'},
      {id: 'max', title: 'Max (ms)'},
      {id: 'min', title: 'Min (ms)'},
      {id: 'avg', title: 'Avg (ms)'}
    ]
  });
  csvWriter.writeRecords(data).then(()=> console.log('The CSV summary file was written successfully'));
}

// Create Processed Log
function calculateProcessedData(data) {
  var s = new Date(data.timestamp).toISOString()
  var statdict = {
    timestamp: s,
    EnqueuedTime: data.IoTHub.EnqueuedTime,
    EventEnqueuedUtcTime: data.EventEnqueuedUtcTime,
    EventProcessedUtcTime: data.EventProcessedUtcTime,
    BufferSize: (data.values).length,
    latency: calculateDeltaGatewaytoHub(data)
  }
  return statdict;
}

function calculateProcessed(data) {
  var calculation = [];

  for (var i = 0; i < data.length; i++) {
    calculation.push(calculateProcessedData(data[i]));
  }

  return(calculation);
}

function export_processedcsv(data) {
  const csvWriter = createCsvWriter({
    path: 'Message_Log.csv',
    header: [
      {id: 'timestamp', title: 'Msg_id (timestamp)'},
      {id: 'EnqueuedTime', title: 'Arrival at IoT Hub (EnqueuedTime)'},
      {id: 'EventEnqueuedUtcTime', title: 'Arrival at Event Hub (EventEnqueuedUtcTime)'},
      {id: 'EventProcessedUtcTime', title: 'Process by SA (EventProcessedUtcTime)'},
      {id: 'BufferSize', title: 'Buffer Size'},
      {id: 'latency', title: 'Latency (EnqueuedTime - Timestamp) (ms)'}
    ]
  });
  csvWriter.writeRecords(data).then(()=> console.log('The CSV log file was written successfully'));
}

async function execute() {
  const aborter = AbortController.timeout(30 * ONE_MINUTE);
  const containerName = CONTAINER_NAME;

  const credentials = new StorageSharedKeyCredential(STORAGE_ACCOUNT_NAME, ACCOUNT_ACCESS_KEY);

  const blobServiceClient = new BlobServiceClient(`https://${STORAGE_ACCOUNT_NAME}.blob.core.windows.net`,credentials);

  const containerClient = blobServiceClient.getContainerClient(containerName);

  var blobName = await getBlobName(aborter, containerClient);
  const blobClient = containerClient.getBlobClient(blobName);
  const blockBlobClient = blobClient.getBlockBlobClient();

  console.log("Containers:");
  await showContainerNames(aborter, blobServiceClient);

  console.log(`Blobs in "${containerName}" container:`);
  await showBlobNames(aborter, containerClient);

  console.log(`Processing: ${blobName}\n`)

  const downloadResponse = await blockBlobClient.download(0,aborter);
  const downloadedContent = await streamToString(downloadResponse.readableStreamBody);
  var azure_data = JSON.parse(downloadedContent+ "]");

  var processed = calculateProcessed(azure_data);
  export_processedcsv(processed);
  var summary = calculateSummary(azure_data,mqtt_data);
  export_summarycsv(summary);
}

execute().then(() => console.log("Done")).catch((e) => console.log(e));