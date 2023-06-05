package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.KafkaService;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class EmailNewOrderService {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var emailNewOrderService = new EmailNewOrderService();
        try(var service = new KafkaService<Order>(EmailNewOrderService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
                emailNewOrderService::parse, Map.of())){
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        var emailDispatcher = new KafkaDispatcher<String>();
        System.out.println("key " + record.key());
        Message<Order> message = record.value();
        System.out.println("value " + message);
        System.out.println("offset " + record.offset());
        System.out.println("partition " + record.partition());


        var emailCode = "Thank you for you order| Me are";
        Order order = message.getPayload();
        CorrelationId correlationId = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());
        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(), correlationId, emailCode);

    }

}
