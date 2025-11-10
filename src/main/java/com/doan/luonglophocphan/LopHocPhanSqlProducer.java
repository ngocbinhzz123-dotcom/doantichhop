package com.doan.luonglophocphan;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class LopHocPhanSqlProducer {

    private final static String QUEUE_NAME = "lophocphan_queue";

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

            // Đọc các cột cần thiết từ Bảng Nguồn 2
            // Ví dụ: MaLopHP, TenLopHP, MaMon, MaGV, HocKy, NamHoc
            String sqlSelect = "SELECT MaLopHP, TenLopHP, MaMon, MaGV, HocKy, NamHoc FROM Bang_LopHocPhan";
            ResultSet rs = stmt.executeQuery(sqlSelect);

            while (rs.next()) {
                // Xây dựng message
                String message = String.join(",",
                        rs.getString("MaLopHP"),
                        rs.getString("TenLopHP"),
                        rs.getString("MaMon"),
                        rs.getString("MaGV"),
                        rs.getString("HocKy"),
                        rs.getString("NamHoc")
                );

                // Thêm tiền tố "SQL"
                message = "SQL," + message;

                rabbitChannel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [SQL-LHP] Đã gửi: '" + rs.getString("MaLopHP") + "'");
            }

            System.out.println("Hoàn tất gửi dữ liệu LopHocPhan từ SQL Nguồn 2!");
        }
    }
}
