package com.doan.luongdiem;

import com.opencsv.CSVReader;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class DiemCsvProducer {

    private final static String QUEUE_NAME = "diem_queue";

    public static void main(String[] argv) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(QUEUE_NAME, true, false, false, null);

            // 👉 Đường dẫn file CSV của bạn
            String csvFilePath = "D:\\dowload\\fileexcel\\diem.csv";

            // Sử dụng InputStreamReader với UTF-8
            try (CSVReader reader = new CSVReader(
                    new InputStreamReader(new FileInputStream(csvFilePath), StandardCharsets.UTF_8))) {

                String[] header = reader.readNext(); // Bỏ header
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
                    System.out.println(" [CSV-DIEM] Đã gửi: '" + line[0] + "'");
                }
            }

            System.out.println("✅ Hoàn tất gửi dữ liệu Điểm từ CSV!");
        }
    }
}
