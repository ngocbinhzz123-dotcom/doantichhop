package com.doan.luongsinhvien;
//File: SinhVienCsvProducer.java
import com.opencsv.CSVReader;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.FileReader;
import java.nio.charset.StandardCharsets;

public class SinhVienCsvProducer {

    private final static String QUEUE_NAME = "sinhvien_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost"); // RabbitMQ server

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            
            //Khai báo Queue
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);

            // TODO:Thay đổi đường dẫn này cho đúng với file của bạn
            String csvFilePath = "D:\\dowload\\fileexcel\\sinhvien.csv"; 

            try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
                
                String[] header = reader.readNext(); // Bỏ qua dòng tiêu đề (header)
                
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
                    System.out.println(" [CSV-SV] Đã gửi: '" + line[0] + "'");
                }
            }
            System.out.println("Hoàn tất gửi dữ liệu SinhVien từ CSV!");
        }
    }
}