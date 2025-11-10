package com.doan.luonggiangvien;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class GiangVienSqlProducer {

    private final static String QUEUE_NAME = "giangvien_queue";

    // Kết nối DB Nguồn 2
    private static final String DB_NGUON_2_URL = "jdbc:sqlserver://DESKTOP-2EBIQK\\SQLEXPRESS:1433;databaseName=QuanLyDiem_SQL;encrypt=false;";
    private static final String DB_USER = "sa1";
    private static final String DB_PASS = "chien123";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost"); // RabbitMQ server

        try (Connection rabbitConnection = factory.newConnection();
             Channel rabbitChannel = rabbitConnection.createChannel();
             java.sql.Connection sqlConnection = DriverManager.getConnection(DB_NGUON_2_URL, DB_USER, DB_PASS);
             Statement stmt = sqlConnection.createStatement()) {

            rabbitChannel.queueDeclare(QUEUE_NAME, true, false, false, null);

            // Đọc 3 cột từ Bảng Nguồn 2 (MaGV, HoTen, MaKhoa)
            String sqlSelect = "SELECT MaGV, HoTen, MaKhoa FROM Bang_GiangVien";
            ResultSet rs = stmt.executeQuery(sqlSelect);

            while (rs.next()) {
                // Xây dựng message (3 cột)
                String message = String.join(",",
                        rs.getString("MaGV"),
                        rs.getString("HoTen"),
                        rs.getString("MaKhoa")
                );

                // Thêm tiền tố "SQL"
                message = "SQL," + message;

                rabbitChannel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [SQL-GV] Đã gửi: '" + rs.getString("MaGV") + "'");
            }

            System.out.println("Hoàn tất gửi dữ liệu GiangVien từ SQL Nguồn 2!");
        }
    }
}
