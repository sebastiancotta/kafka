package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ReadingReportService {

    private static final Path SOURCE = new File("src/main/resource/report.txt").toPath();

    public static void main(String[] args) {
        var readingReportService = new ReadingReportService();
        try(var service = new KafkaService<User>(ReadingReportService.class.getSimpleName(), "USER_GENERATE_READING_REPORT",
                readingReportService::parse, User.class, Map.of())){
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, User> record) throws IOException {
        var orderDispatcher = new KafkaDispatcher<User>();
        System.out.println("processing report for " + record.value());
        var user = record.value();

        var target = new File(user.getReportPath());

        IO.copyTo(SOURCE, target);
        IO.append(target, "Create for " + user.getUuid());
        System.out.println("File created " + target.getAbsolutePath());
    }

}
