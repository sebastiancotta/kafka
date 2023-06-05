package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.KafkaService;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var fraudDetectorService = new FraudDetectorService();
        try(var service = new KafkaService<Order>(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse, Map.of())){
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        var orderDispatcher = new KafkaDispatcher<Order>();
        System.out.println("key " + record.key());
        System.out.println("value " + record.value());
        System.out.println("offset " + record.offset());
        System.out.println("partition " + record.partition());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
           e.printStackTrace();
        }

        var message = record.value();
        var order = message.getPayload();

        if (isFraud(order)) {
            //pretending the fraud happen when the amount is >= 4500, maior ou igual a 4500
            System.out.println("Order is a Fraud: ");
            orderDispatcher.send("ECOMMERCE_ORDER_REJECT", order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        } else {
            System.out.println("Approved: " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()),order);
        }

    }

    private static boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal(("450"))) >= 0;
    }
}
