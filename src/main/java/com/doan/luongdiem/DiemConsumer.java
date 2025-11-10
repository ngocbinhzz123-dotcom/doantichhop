package com.doan.luongdiem;

import com.rabbitmq.client.*;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DiemConsumer {

    private final static String QUEUE_NAME = "diem_queue";
    private static final String DB_URL = "jdbc:sqlserver://LAPTOPCUABINH:1433;databaseName=QuanLyDiem_STAGING;encrypt=false;";
    private static final String DB_USER = "doan_app";
    private static final String DB_PASS = "123456";

    private static final String SQL_INSERT = "INSERT INTO STAGING_DIEM " +
            "(DiemID, MaSV, MaLopHP, DiemPhatBieu, DiemBaiTap, DiemChuyenCan, DiemGiuaKy, DiemDoAn, DiemCuoiKi, NguonDuLieu, TrangThaiQC) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')";

    public static void main(String[] argv) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection rabbitConnection = factory.newConnection();
        Channel channel = rabbitConnection.createChannel();

        java.sql.Connection sqlConnection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);

        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Đang chờ message [DIEM] để lưu vào STAGING...");

        final java.sql.Connection finalSqlConnection = sqlConnection;

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            String nguon = message.substring(0, 3);
            String payload = message.substring(4);
            String[] data = payload.split(",");

            try (PreparedStatement ps = finalSqlConnection.prepareStatement(SQL_INSERT)) {
                ps.setString(1, data[0]); // DiemID
                ps.setString(2, data[1]); // MaSV
                ps.setString(3, data[2]); // MaLopHP
                ps.setString(4, data[3]); // DiemPhatBieu
                ps.setString(5, data[4]); // DiemBaiTap
                ps.setString(6, data[5]); // DiemChuyenCan
                ps.setString(7, data[6]); // DiemGiuaKy
                ps.setString(8, data[7]); // DiemDoAn
                ps.setString(9, data[8]); // DiemCuoiKi
                ps.setString(10, nguon);  // NguonDuLieu

                ps.executeUpdate();
                System.out.println("✅ Đã lưu vào STAGING_DIEM: " + data[0]);

            } catch (SQLException e) {
                System.err.println("❌ Lỗi SQL khi INSERT: " + e.getMessage());
            }
        };

        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
        });
    }
}
