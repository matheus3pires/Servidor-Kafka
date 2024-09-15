package pires.mensageria;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Collections;
import java.util.Properties;

public class ConfirmarPedido {
    private JButton confirmarButton;
    private JButton cancelarButton;
    private JPanel PiresPage;
    static String value = "";
    static JFrame quadro = new JFrame("Pires");

    public ConfirmarPedido() {
        String topicName = "servidor-pires";
        Properties propsConsumer = new Properties();
        propsConsumer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsConsumer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsConsumer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        confirmarButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                value = "Confirmado";
                KafkaProducer<String, String> producer = new KafkaProducer<>(propsConsumer);
                ProducerRecord<String, String> recordProducer = new ProducerRecord<>(topicName, value);
                producer.send(recordProducer);

                System.out.println("Status: " + recordProducer.value());
                quadro.dispose();
            }
        });

        cancelarButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                value = "Cancelado";
                KafkaProducer<String, String> producer = new KafkaProducer<>(propsConsumer);
                ProducerRecord<String, String> recordProducer = new ProducerRecord<>(topicName, value);
                producer.send(recordProducer);

                System.out.println("Status: " + recordProducer.value());
                quadro.dispose();
            }
        });
    }

    public static void main(String[] args) {
        String topicName = "ecommerce-pires";
        Properties propsConsumer = new Properties();

        propsConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, topicName);
        propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsConsumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(propsConsumer);
        consumer.subscribe(Collections.singleton(topicName));
        new Thread(() -> {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Pedido Recebido => " + record.value());
                    quadro.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
                    quadro.setContentPane(new ConfirmarPedido().PiresPage);
                    quadro.setSize(500, 500);
                    quadro.setVisible(true);
                    quadro.setLocationRelativeTo(null);
                    quadro.pack();
                }
            }
        }).start();
    }
}
