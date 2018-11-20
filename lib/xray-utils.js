const xray = require('aws-xray-sdk-core');
const Segment = xray.Segment;

const SEPARATOR = "|";

var utils = {
  captureProducer: function captureProducer(producer, message) {
    //trace through producer
    let subsegment = xray.getSegment().addNewSubsegment("ProducerSend");

    subsegment.addMetadata("client", producer.client.constructor.name);
    subsegment.addMetadata("client_version", producer.client.serverInfo.redis_version);
    subsegment.addMetadata("host", producer.client.options.host);
    subsegment.addMetadata("port", producer.client.options.port);
    subsegment.addMetadata("message", message);
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
    segment.addMetadata("message", m);
    segment.addMetadata("client", message[1]);

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