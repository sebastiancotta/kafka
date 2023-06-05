package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.KafkaService;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {

    private final Connection connection;

    public BatchSendMessageService() throws SQLException {
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

    public static void main(String[] args) throws ExecutionException, InterruptedException , SQLException {
        var batchService = new BatchSendMessageService();
        try (var service = new KafkaService<>(BatchSendMessageService.class.getSimpleName(),
                "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                batchService::parse,
                Map.of())) {
            service.run();
        }
    }
    private final KafkaDispatcher<User> userKafkaDispatcher = new KafkaDispatcher<>();
    private void parse(ConsumerRecord<String, Message<String>> record ) throws Exception {
        var message = record.value();
        System.out.println("Process new batch");
        System.out.println("Topic " + message.getPayload());

        for (User user : getAllUsers()) {
            userKafkaDispatcher.send(message.getPayload(), user.getUuid(),  message.getId().continueWith(BatchSendMessageService.class.getSimpleName()), user);
        }

    }

    private List<User> getAllUsers() throws SQLException {
        PreparedStatement preparedStatement = this.connection.prepareStatement("select uuid from users ");
        List<User> users = new ArrayList<>();
        var result = preparedStatement.executeQuery();
        while ((result.next())) {
            users.add(new User(result.getString(1)));
        }
        return users;
    }
}
