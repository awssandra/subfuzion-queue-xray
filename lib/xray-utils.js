const xray = require('aws-xray-sdk-core');
const Segment = xray.Segment;

const SEPARATOR = "|";

let captureClient = function captureClient(segment, client, message) {
  segment.addMetadata("client", client.constructor.name);
  segment.addMetadata("client_version", client.serverInfo.redis_version);
  segment.addMetadata("host", client.options.host);
  segment.addMetadata("port", client.options.port);
  segment.addMetadata("message", message);
};

var utils = {
  captureProducer: function captureProducer(producer, message) {
    //trace through producer
    let subsegment = xray.getSegment().addNewSubsegment("ProducerSend");

    captureClient(subsegment, producer.client, message);
    subsegment.namespace = 'remote';

    return subsegment;
  },
  captureConsumer: function captureConsumer(consumer, message) {
    //trace through consumer
    let header = JSON.parse(message[1]).xray;
    let m = JSON.parse(message[1]).message;
    header = xray.utils.processTraceData(header);

    let segment = new Segment("ConsumerReceive", header.Root, header.Parent);
    segment.traced = !!header.Sampled;

    captureClient(segment, consumer.client, m);

    let namespace = xray.getNamespace();
    namespace.enter(namespace.createContext());
    xray.setSegment(segment);
    
    return [message[0], JSON.stringify(m)];
  },
  appendXRayMessage: function appendXRayMessage(subsegment, message) {
    let root = subsegment.segment;
    let header = 'Root=' + root.trace_id + ';Parent=' + subsegment.id + ';Sampled=' + (!root.notTraced ? '1' : '0');

    let xrayMessage = {
      "xray": header,
      "message": message
    };

    return xrayMessage;
  }
};

module.exports = utils;