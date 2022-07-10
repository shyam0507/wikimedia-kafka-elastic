import { Kafka, Producer } from 'kafkajs';

export const getProducer = async () => {
  const kafka = new Kafka({
    clientId: 'producer-client',
    brokers: ['localhost:9092'],
  });

  const producer: Producer = kafka.producer();
  await producer.connect();

  return producer;
};
