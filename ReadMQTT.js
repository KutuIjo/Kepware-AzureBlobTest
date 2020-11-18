const fs = require('fs');
var mqtt = require('mqtt');
var client  = mqtt.connect('tcp://localhost:1883');
var mqtt_topic = "iotgateway";

var jsonArray = [];
var filePath = "MQTTresult.json";

client.on('connect', function () {
  client.subscribe(mqtt_topic);
})
 
client.on('message', function (topic, message) {
  // message is Buffer
  var data = JSON.parse(message.toString());
  console.log(`Data:"${data}"`);
  jsonArray.push(data);
  // console.log(`Data:"${jsonArray}"`);
  fs.writeFile(filePath, JSON.stringify(jsonArray), function (err) {
     if (err) throw err;
     console.log('The "data to append" was appended to file!');
  });
})
