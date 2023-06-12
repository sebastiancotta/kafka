package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailNewOrderService implements ConsumerService<Order> {

    public static void main(String[] args) {
        new ServiceRunner<>(EmailNewOrderService::new).start(1);
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws Exception {
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
