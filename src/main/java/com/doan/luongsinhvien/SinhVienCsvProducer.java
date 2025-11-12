package com.doan.luongsinhvien;

import com.opencsv.CSVReader;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class SinhVienCsvProducer {

    private final static String QUEUE_NAME = "sinhvien_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost"); // RabbitMQ server

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Khai báo Queue
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);

            // Đường dẫn CSV
            String csvFilePath = "D:\\dowload\\fileexcel\\sinhvien.csv";

            // Sử dụng InputStreamReader với UTF-8
            try (CSVReader reader = new CSVReader(
                    new InputStreamReader(new FileInputStream(csvFilePath), StandardCharsets.UTF_8))) {

                reader.readNext(); // Bỏ header
                String[] line;
                while ((line = reader.readNext()) != null) {
                    String payload = String.join(",", line);

                    // Bỏ qua dòng trống
                    if (payload.isEmpty() || payload.matches("^,+$")) {
                        continue;
                    }

                    // Thêm tiền tố "CSV,"
                    String message = "CSV," + payload;

                    // Gửi message qua RabbitMQ với UTF-8
                    channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                    System.out.println(" [CSV-SV] Đã gửi: '" + line[0] + "'");
                }
            }

            System.out.println("✅ Hoàn tất gửi dữ liệu SinhVien từ CSV!");
        }
    }
}
