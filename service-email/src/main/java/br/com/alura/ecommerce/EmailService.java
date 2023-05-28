package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class EmailService {

    public static void main(String[] args) {
        var emailService = new EmailService();
        try(var service = new KafkaService<String>(EmailService.class.getSimpleName(), "ECOMMERCE_SEND_EMAIL", emailService::parse,
                String.class, Map.of())){
            service.run();
        }

    }
    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("key " + record.key());
        System.out.println("value " + record.value());
        System.out.println("offset " + record.offset());
        System.out.println("partition " + record.partition());
    }
}