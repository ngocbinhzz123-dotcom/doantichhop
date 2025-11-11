package com.doan.khoa;

import com.doan.util.ketnoi;
import com.opencsv.CSVReader;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;

public class KhoaCsvProducer {

    private final static String QUEUE_NAME = "khoa_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);

            // TODO: Sửa lại đường dẫn file này
            String csvFilePath = "D:\\dowload\\fileexcel\\khoa.csv"; 

            try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
                String[] header = reader.readNext(); 
                
                String[] line;
                while ((line = reader.readNext()) != null) {
                    
                    String payload = String.join(",", line);
                    if (payload.isEmpty() || payload.matches("^,+$")) continue;
                    
                    String message = "CSV," + payload; 

                    channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                    System.out.println(" [CSV-KHOA] Đã gửi: '" + line[0] + "'");
                }
            }
            System.out.println("Hoàn tất gửi dữ liệu Khoa từ CSV!");
        }
    }
}