package com.doan.luongdiem;

import com.doan.util.DBConnetor;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DiemConsumer {

    private final static String QUEUE_NAME = "diem_queue";
    
    // (Đã xóa URL, USER, PASS)
    
    private static final String SQL_INSERT_STAGING = "INSERT INTO STAGING_DIEM " +
        "(MaSV, MaLopHP, DiemPhatBieu, DiemBaiTap, DiemChuyenCan, DiemGiuaKy, DiemDoAn, DiemCuoiKi, NguonDuLieu, TrangThaiQC) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')";

    public static void main(String[] argv) throws Exception {
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection rabbitConnection = factory.newConnection();
        Channel channel = rabbitConnection.createChannel();
        java.sql.Connection sqlConnection = null;

        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Đang chờ message [Diem] để lưu vào STAGING...");

        try {
            sqlConnection = DBConnetor.getConnectionDich(); // Đã sửa
            System.out.println("Ket noi DB Dich [QuanLyDiemSV] thanh cong!");
            
            final java.sql.Connection finalSqlConnection = sqlConnection;

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                
                try (PreparedStatement ps = finalSqlConnection.prepareStatement(SQL_INSERT_STAGING)) {
                    
                    String nguon = message.substring(0, 3);
                    String payloadStr = message.substring(4);
                    String[] data = payloadStr.split(",");
                    
                    ps.setString(1, data[0]); // MaSV
                    ps.setString(2, data[1]); // MaLopHP
                    ps.setString(3, data[2]); // DiemPhatBieu
                    ps.setString(4, data[3]); // DiemBaiTap
                    ps.setString(5, data[4]); // DiemChuyenCan
                    ps.setString(6, data[5]); // DiemGiuaKy
                    ps.setString(7, data[6]); // DiemDoAn
                    ps.setString(8, data[7]); // DiemCuoiKi
                    ps.setString(9, nguon);   // NguonDuLieu
                    
                    ps.executeUpdate();
                    System.out.println("    -> Đã lưu vào STAGING_DIEM: " + data[0] + " - " + data[1]);

                } catch (SQLException e) {
                    System.err.println(" ! Lỗi SQL khi INSERT vào STAGING: " + e.getMessage());
                } catch (Exception e) {
                    System.err.println(" ! Lỗi xử lý message: " + e.getMessage());
                }
            };
            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
            
        } catch (SQLException e) { 
            System.err.println("Lỗi nghiêm trọng: Không thể kết nối đến DB Đích.");
            e.printStackTrace();
            if (sqlConnection != null) sqlConnection.close();
            if (channel != null) channel.close();
            if (rabbitConnection != null) rabbitConnection.close();
        }
    }
}