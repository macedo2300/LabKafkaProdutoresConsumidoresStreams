package br.com.vms.ecommerce.service;

import br.com.vms.ecommerce.interfaces.ConsumerFunction;
import br.com.vms.ecommerce.util.GsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaConsumerService<T> implements Closeable {

    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;


    public KafkaConsumerService(String groupId, String topic, ConsumerFunction parse, Class<T> type,Map<String,String> properties) {
        this(parse,groupId,type,properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaConsumerService(String groupId, Pattern topic, ConsumerFunction parse,Class<T> type,Map<String,String> properties) {
        this(parse,groupId,type,properties);
        consumer.subscribe(topic);
    }

    public KafkaConsumerService(ConsumerFunction parse,String groupId,Class<T> type,Map<String,String> properties) {
        this.consumer = new KafkaConsumer<>(getProperties(type,groupId,properties));
        this.parse = parse;
    }


    public void run() {
        while(true){
            var records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()){
                System.out.println("Encontrei " +records.count());
                for(var record: records){
                    parse.consume(record);
                }
            }
        }
    }

    private Properties getProperties(Class<T> type, String groupId, Map<String,String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        this.consumer.close();
    }
}
