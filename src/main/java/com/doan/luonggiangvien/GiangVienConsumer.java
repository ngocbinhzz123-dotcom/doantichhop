package com.doan.luonggiangvien;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class GiangVienConsumer {

    private final static String QUEUE_NAME = "giangvien_queue";
    
    private static final String DB_DICH_URL = "jdbc:sqlserver://DESKTOP-2EBIQK\\SQLEXPRESS:1433;databaseName=QuanLyDiem_STAGING;encrypt=false;";
    private static final String DB_USER = "sa1";
    private static final String DB_PASS = "chien123";
    
    private static final String SQL_INSERT_STAGING = "INSERT INTO STAGING_GIANGVIEN " +
            "(MaGV, HoTen, Email, SDT, NguonDuLieu, TrangThaiQC) " +
            "VALUES (?, ?, ?, ?, ?, 'PENDING')";

    public static void main(String[] argv) throws Exception {
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        
        Connection rabbitConnection = factory.newConnection();
        Channel channel = rabbitConnection.createChannel();
        java.sql.Connection sqlConnection = null;

        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Đang chờ message [GiangVien] để lưu vào STAGING...");

        try {
            sqlConnection = DriverManager.getConnection(DB_DICH_URL, DB_USER, DB_PASS);
            final java.sql.Connection finalSqlConnection = sqlConnection;

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                String[] data = message.split(",");

                try (PreparedStatement ps = finalSqlConnection.prepareStatement(SQL_INSERT_STAGING)) {
                    ps.setString(1, data[0]); // MaGV
                    ps.setString(2, data[1]); // HoTen
                    ps.setString(3, data.length > 2 ? data[2] : null); // Email
                    ps.setString(4, data.length > 3 ? data[3] : null); // SDT
                    ps.setString(5, "CSV"); // NguonDuLieu
                    
                    ps.executeUpdate();
                    System.out.println("    -> Đã lưu vào STAGING_GIANGVIEN: " + data[0]);
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
