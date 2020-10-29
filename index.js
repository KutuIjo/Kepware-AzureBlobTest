const {
    StorageSharedKeyCredential,
    BlobServiceClient
    } = require('@azure/storage-blob');
const {AbortController} = require('@azure/abort-controller');

if (process.env.NODE_ENV !== "production") {
    require("dotenv").config();
}

const STORAGE_ACCOUNT_NAME = process.env.AZURE_STORAGE_ACCOUNT_NAME;
const ACCOUNT_ACCESS_KEY = process.env.AZURE_STORAGE_ACCOUNT_ACCESS_KEY;
const CONTAINER_NAME = process.env.AZURE_STORAGE_CONTAINER_NAME;
const BLOB_FILEPATH = process.env.AZURE_STORAGE_BLOB_FILEPATH;

const ONE_MINUTE = 60 * 1000;

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
      console.log(` - ${blob.name}`);
    }
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

function parseBlobContent(downloadedString) {
  var result = "["+downloadedString;
  var startlocation = 1;
  var loglocation = 1;
  while (loglocation != -1) {
    loglocation = result.indexOf("Z\"}}", startlocation);
    console.log(loglocation);
    result = result.splice((loglocation + 4), 0, ",");
    startlocation = loglocation + 5;
  }
  result = "[{\"" + result.slice(4, -1) + "]";
  data = JSON.parse(result);
  return data;   
}

async function execute() {
    const containerName = CONTAINER_NAME;
    const blobName = BLOB_FILEPATH;

    const credentials = new StorageSharedKeyCredential(STORAGE_ACCOUNT_NAME, ACCOUNT_ACCESS_KEY);

    const blobServiceClient = new BlobServiceClient(`https://${STORAGE_ACCOUNT_NAME}.blob.core.windows.net`,credentials);

    const containerClient = blobServiceClient.getContainerClient(containerName);
    const blobClient = containerClient.getBlobClient(blobName);
    const blockBlobClient = blobClient.getBlockBlobClient();
    const aborter = AbortController.timeout(30 * ONE_MINUTE);

    console.log("Containers:");
    await showContainerNames(aborter, blobServiceClient);

    console.log(`Blobs in "${containerName}" container:`);
    await showBlobNames(aborter, containerClient);

    const downloadResponse = await blockBlobClient.download(0,aborter);
    const downloadedContent = await streamToString(downloadResponse.readableStreamBody);

    var data = parseBlobContent(downloadedContent);
    console.log(`Data: "${data}"`);

    var statistics = calculateData(data);
}

execute().then(() => console.log("Done")).catch((e) => console.log(e));
