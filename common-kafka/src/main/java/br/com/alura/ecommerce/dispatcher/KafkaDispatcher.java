package br.com.alura.ecommerce.dispatcher;

import br.com.alura.ecommerce.CorrelationId;
import br.com.alura.ecommerce.Message;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaDispatcher<T> implements Closeable {
    private final KafkaProducer<String, Message<T>> producer;


    public KafkaDispatcher() {
        this.producer = new KafkaProducer<>(properties());
    }

    public void send(String topic, String key, CorrelationId id, T payload) throws ExecutionException, InterruptedException {
        Future<RecordMetadata> future = sendAsync(topic, key, id, payload);
        future.get();
    }

    private Future<RecordMetadata> sendAsync(String topic, String key, CorrelationId id, T payload) {
        var value = new Message<T>(id.continueWith("-" + topic), payload);
        var producerRecord = new ProducerRecord<>(topic, key, value);
        Callback callback = (data, ex) -> {
            if (null != ex) {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso em enviar" + data.topic() + " ::: partion " + data.partition() + " / offset " + data.offset() + " timestamp " + data.timestamp());
        };
        Future<RecordMetadata> future = producer.send(producerRecord, callback);
        return future;
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    @Override
    public void close() {
        this.producer.close();
    }
}
