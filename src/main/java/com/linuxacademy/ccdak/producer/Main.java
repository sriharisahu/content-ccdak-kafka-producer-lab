package com.linuxacademy.ccdak.producer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Main {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        props.put("acks", "all");
        
        Producer<String, String> producer = new KafkaProducer<>(props);
        
        try{
            List<String> lines = Files.readAllLines(Path.of(ClassLoader.getSystemResource("sample_transaction_log.txt").toURI()), Charset.forName("UTF-8"));
            for(String line: lines){
                String[] lineArray = line.split(":");
                String key = lineArray[0];
                String value = lineArray[1];
                producer.send(new ProducerRecord<>("inventory_purchases", key, value));
                if (key.equals("apples")) {
                    producer.send(new ProducerRecord<>("apple_purchases", key, value));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        
        producer.close();
    }

}
