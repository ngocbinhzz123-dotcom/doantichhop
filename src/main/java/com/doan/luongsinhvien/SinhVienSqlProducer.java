package com.doan.luongsinhvien;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class SinhVienSqlProducer {

 private final static String QUEUE_NAME = "sinhvien_queue";
 
 // TODO: Sửa tên DB Nguồn 2 nếu khác
 private static final String DB_NGUON_2_URL = "jdbc:sqlserver://LAPTOPCUABINH:1433;databaseName=QuanLyDiem_SQL;encrypt=false;";
 private static final String DB_USER = "doan_app";
 private static final String DB_PASS = "123456";
 
 public static void main(String[] argv) throws Exception {
     ConnectionFactory factory = new ConnectionFactory();
     factory.setHost("localhost"); // RabbitMQ server

     try (Connection rabbitConnection = factory.newConnection();
          Channel rabbitChannel = rabbitConnection.createChannel();
          
          // Kết nối DB Nguồn 2
    	 java.sql.Connection sqlConnection = DriverManager.getConnection(DB_NGUON_2_URL, DB_USER, DB_PASS);
         Statement stmt = sqlConnection.createStatement()) {

         rabbitChannel.queueDeclare(QUEUE_NAME, true, false, false, null);

         // Đọc 6 cột từ Bảng Nguồn 2 (không có Email, SDT)
         String sqlSelect = "SELECT MaSV, HoTen, NgaySinh, GioiTinh, MaLop, MaKhoa FROM Bang_SinhVien";
         ResultSet rs = stmt.executeQuery(sqlSelect);

         while (rs.next()) {
             // Xây dựng message (6 cột)
             String message = String.join(",", 
                 rs.getString("MaSV"),
                 rs.getString("HoTen"),
                 rs.getString("NgaySinh"), // Định dạng: YYYY-MM-DD
                 rs.getString("GioiTinh"),
                 rs.getString("MaLop"),
                 rs.getString("MaKhoa")
             );

             rabbitChannel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
             System.out.println(" [SQL-SV] Đã gửi: '" + rs.getString("MaSV") + "'");
         }
         
         System.out.println("Hoàn tất gửi dữ liệu SinhVien từ SQL Nguồn 2!");
     }
 }
}
