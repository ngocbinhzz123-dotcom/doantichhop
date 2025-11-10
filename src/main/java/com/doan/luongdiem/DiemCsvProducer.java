package com.doan.luongdiem;

import com.opencsv.CSVReader;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.FileReader;
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

            try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {

                String[] header = reader.readNext(); // Bỏ qua dòng tiêu đề
                String[] line;
                while ((line = reader.readNext()) != null) {

                    // Ghép các cột lại thành 1 message
                    String payload = String.join(",", line);
                    String message = "CSV," + payload;

                    channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                    System.out.println(" [CSV-DIEM] Đã gửi: '" + line[0] + "'");
                }
            }

            System.out.println("✅ Hoàn tất gửi dữ liệu Điểm từ CSV!");
        }
    }
}
