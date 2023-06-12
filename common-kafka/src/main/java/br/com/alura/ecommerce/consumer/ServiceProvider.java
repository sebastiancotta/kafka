package br.com.alura.ecommerce.consumer;

import java.util.Map;
import java.util.concurrent.Callable;

public class ServiceProvider<T> implements Callable<Void> {

    private final ServiceFactory<T> factory;

    public ServiceProvider(ServiceFactory<T> factory) {
        this.factory = factory;
    }

    public Void call() throws Exception {
        var myService = factory.create();
        try (var service = new KafkaService<T>(myService.getConsumerGroup()
                , myService.getTopic(), record -> {
            try {
                myService.parse(record);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, Map.of())) {
            service.run();
        }
        return null;
    }
}
