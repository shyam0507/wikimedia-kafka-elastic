import { EachMessagePayload } from 'kafkajs';
const { Client } = require('@elastic/elasticsearch');
import { getConsumer } from './consumer';
import { getProducer } from './producer';
var EventSource = require('eventsource');
var es = new EventSource('https://stream.wikimedia.org/v2/stream/recentchange');

const start = async () => {
  const producer = await getProducer();
  es.on('message', async (data: any, err: any) => {
    const payload = JSON.parse(data.data);
    console.log('Received Data: ', payload);
    await producer.send({
      topic: 'wikimedia.recentchanges',
      messages: [{ value: JSON.stringify(payload) }],
    });
  });

  //Create the elastic search client

  const elsticClient = new Client({
    node: 'http://localhost:9200',
  });

  const consumer = await getConsumer();
  await consumer.run({
    eachBatchAutoResolve: false,
    eachMessage: async (messagePayload: EachMessagePayload) => {
      const { topic, partition, message } = messagePayload;
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      console.log(message);

      await elsticClient.index({
        index: 'wikimedia_recentchanges',
        document: message,
      });
    },
  });
};

start();
