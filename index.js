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

const ONE_MINUTE = 60 * 1000;

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

async function execute() {

    const containerName = "<containername>";
    const blobName = "<filepathtoblob>";

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

    console.log(`Downloaded blob content: "${downloadedContent}"`);
}

execute().then(() => console.log("Done")).catch((e) => console.log(e));
