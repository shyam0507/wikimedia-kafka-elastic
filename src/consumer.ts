import { ConsumerSubscribeTopic, Kafka } from 'kafkajs';

export const getConsumer = async () => {
  const kafka = new Kafka({
    clientId: 'consumer-client',
    brokers: ['localhost:9092'],
  });

  const consumer = kafka.consumer({ groupId: 'my-group' });

  const subscription: ConsumerSubscribeTopic = {
    topic: 'wikimedia.recentchanges',
    fromBeginning: false,
  };

  await consumer.connect();
  await consumer.subscribe(subscription);

  return consumer;
};
