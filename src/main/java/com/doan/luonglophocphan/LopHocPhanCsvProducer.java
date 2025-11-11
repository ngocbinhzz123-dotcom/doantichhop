package com.doan.luonglophocphan;

import com.opencsv.CSVReader;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.FileReader;
import java.nio.charset.StandardCharsets;

public class LopHocPhanCsvProducer {

    private final static String QUEUE_NAME = "lophocphan_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(QUEUE_NAME, true, false, false, null);

            String csvFilePath = "D:\\dowload\\fileexcel\\lop.csv"; 

            try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
                reader.readNext(); // bỏ header
                String[] line;
                while ((line = reader.readNext()) != null) {
                    String payload = String.join(",", line);
                    
                    // Check dòng trống (Rất quan trọng)
                    if (payload.isEmpty() || payload.matches("^,+$")) {
                        continue; // Bỏ qua dòng trống
                    }
                    
                    // Thêm tiền tố "CSV,"
                    String message = "CSV," + payload; 
                    
                    channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                    System.out.println(" [CSV-LHP] Đã gửi: " + line[0]);
                }
            }
        }
    }
}
