package br.com.vms.ecommerce;

import br.com.vms.ecommerce.model.Order;
import br.com.vms.ecommerce.service.KafkaConsumerService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

public class FraudDetectorService {

    public static void main(String[] args) {
        var fraudDetectorService = new FraudDetectorService();
        try(var consumer = new KafkaConsumerService<Order>(
                FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse,Order.class,
                new HashMap<>())){
            consumer.run();
        }

    }

    private void parse(ConsumerRecord<String, Order> record) {
        System.out.println("-----------------------------------------");
        System.out.println("Processing new order , checking for fraud");
        System.out.println("Key: " + record.key());
        System.out.println("Valor: " + record.value());
        System.out.println("Partition: "+ record.partition());
        System.out.println("Offset: " + record.offset());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            //ignoring
            e.printStackTrace();
        }
        System.out.println("Order processed");
    }


}
