package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.UUID;

public class CreateUserService implements ConsumerService<Order> {

    private final LocalDatabase dataabse;

    public CreateUserService() throws SQLException {
        this.dataabse = new LocalDatabase("users_database");
        this.dataabse.createNotIfExists("create table Users (" +
                "uuid  varchar(200) primary key," +
                "email varchar(200))");
    }

    public static void main(String[] args) {
        new ServiceRunner<>(CreateUserService::new).start(1);
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return CreateUserService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws Exception {
        System.out.println("Process new order, cheking new user");
        System.out.println("key " + record.key());
        System.out.println("value " + record.value());
        System.out.println("offset " + record.offset());
        System.out.println("partition " + record.partition());

        var order = record.value().getPayload();

        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail(), UUID.randomUUID().toString());
        }

    }

    private void insertNewUser(String email, String uuid) throws SQLException {
        this.dataabse.update("insert into users (uuid, email) values(?,?)", uuid, email);
        System.out.println("usuario adicionado com email: " + email);
    }

    private boolean isNewUser(String email) throws SQLException {
        return !this.dataabse.query("select uuid from users where email = ? limit 1", email).next();
    }
}
