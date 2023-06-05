package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService implements ConsumerService<String> {

    public static void main(String[] args) {
        new ServiceRunner<>(EmailService::new).start(5);
    }

    @Override
    public String getConsumerGroup() {
        return EmailService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_SEND_EMAIL";
    }

    public void parse(ConsumerRecord<String, Message<String>> record) {
        System.out.println("key " + record.key());
        System.out.println("value " + record.value());
        System.out.println("offset " + record.offset());
        System.out.println("partition " + record.partition());
    }
}
