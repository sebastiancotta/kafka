package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.*;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CreateUserService {
    private final Connection connection;

    public CreateUserService() throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:sqlite:service-users/target/users_database.db");
        try {
            this.connection.createStatement().execute("create table Users (" +
                    "uuid  varchar(200) primary key," +
                    "email varchar(200))"
            );
        } catch (SQLException exception) {
            exception.printStackTrace();
        }

    }

    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
        var createUserService = new CreateUserService();
        try(var service = new KafkaService<Order>(CreateUserService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
                createUserService::parse, Map.of())){
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws Exception {
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

    private void insertNewUser(String email, String uuid) throws SQLException  {
        PreparedStatement preparedStatement = this.connection.prepareStatement("insert into users (uuid, email) values(?,?)");
        preparedStatement.setString(1, uuid);
        preparedStatement.setString(2, email);
        preparedStatement.execute();
        System.out.println("usuario adicionado com email: " + email);
    }

    private boolean isNewUser(String email) throws SQLException {
        PreparedStatement exists = this.connection.prepareStatement("select uuid from users where email = ? limit 1");
        exists.setString(1, email);
        ResultSet result = exists.executeQuery();
        return !result.next();

    }
}
