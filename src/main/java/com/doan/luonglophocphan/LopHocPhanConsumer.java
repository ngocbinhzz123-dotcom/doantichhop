package com.doan.luonglophocphan;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LopHocPhanConsumer {

    private final static String QUEUE_NAME = "lophocphan_queue";
    
    private static final String DB_DICH_URL = "jdbc:sqlserver://DESKTOP-2EBIQK\\SQLEXPRESS:1433;databaseName=QuanLyDiem_STAGING;encrypt=false;";
    private static final String DB_USER = "sa1";
    private static final String DB_PASS = "chien123";
    
    private static final String SQL_INSERT_STAGING = "INSERT INTO STAGING_LOPHOCPHAN " +
            "(MaLopHP, MaMon, MaGV, HocKy, NguonDuLieu, TrangThaiQC) " +
            "VALUES (?, ?, ?, ?, ?, 'PENDING')";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection rabbitConnection = factory.newConnection();
        Channel channel = rabbitConnection.createChannel();
        java.sql.Connection sqlConnection = null;

        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Đang chờ message [LopHocPhan] để lưu vào STAGING...");

        try {
            sqlConnection = DriverManager.getConnection(DB_DICH_URL, DB_USER, DB_PASS);
            final java.sql.Connection finalSqlConnection = sqlConnection;

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                String[] data = message.split(",");

                try (PreparedStatement ps = finalSqlConnection.prepareStatement(SQL_INSERT_STAGING)) {
                    ps.setString(1, data[0]); // MaLopHP
                    ps.setString(2, data[1]); // MaMon
                    ps.setString(3, data[2]); // MaGV
                    ps.setString(4, data[3]); // HocKy
                    ps.setString(5, "CSV");  // NguonDuLieu
                    ps.executeUpdate();
                    System.out.println("    -> Đã lưu vào STAGING_LOPHOCPHAN: " + data[0]);
                } catch (SQLException e) {
                    System.err.println(" ! Lỗi SQL khi INSERT: " + e.getMessage());
                }
            };

            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });

        } catch (SQLException e) {
            e.printStackTrace();
            if (sqlConnection != null) sqlConnection.close();
            if (channel != null) channel.close();
            if (rabbitConnection != null) rabbitConnection.close();
        }
    }
}
