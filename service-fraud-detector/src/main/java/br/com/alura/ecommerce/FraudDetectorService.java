package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService implements ConsumerService<Order> {

    private final LocalDatabase database;

    public static void main(String[] args) {
        new ServiceRunner<>(FraudDetectorService::new).start(1);
    }

    public FraudDetectorService() throws SQLException {
        this.database = new LocalDatabase("frauds_database");
        this.database.createNotIfExists("create table Orders (" +
                "uuid  varchar(200) primary key," +
                "is_fraud boolean)");
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return FraudDetectorService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {
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

        if (wasProcessed(order)) {
            System.out.println("Order " + order.getOrderId() + " was already processed!");
            return;
        }

        if (isFraud(order)) {
            //pretending the fraud happen when the amount is >= 4500, maior ou igual a 4500
            database.update("insert into Orders (uuid, is_fraud) values (?, true)", order.getOrderId());
            System.out.println("Order is a Fraud: ");
            orderDispatcher.send("ECOMMERCE_ORDER_REJECT", order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        } else {
            database.update("insert into Orders (uuid, is_fraud) values (?, false)", order.getOrderId());
            System.out.println("Approved: " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        }

    }

    private boolean wasProcessed(Order order) throws SQLException {
        return database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId()).next();
    }

    private static boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal(("450"))) >= 0;
    }
}
