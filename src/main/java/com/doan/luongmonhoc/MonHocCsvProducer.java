package com.doan.luongmonhoc;

import com.opencsv.CSVReader;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.FileReader;
import java.nio.charset.StandardCharsets;

public class MonHocCsvProducer {

private final static String QUEUE_NAME = "monhoc_queue";

public static void main(String[] argv) throws Exception {
   ConnectionFactory factory = new ConnectionFactory();
   factory.setHost("localhost");

   try (Connection connection = factory.newConnection();
        Channel channel = connection.createChannel()) {
       
       channel.queueDeclare(QUEUE_NAME, true, false, false, null);

       // TODO: Thay đổi đường dẫn này
       String csvFilePath = "D:\\dowload\\fileexcel\\monhoc.csv"; 

       try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
           
           String[] header = reader.readNext(); // Bỏ qua tiêu đề
           
           String[] line;
           while ((line = reader.readNext()) != null) {
               
               // Ghép 4 cột (MaMon,TenMon,SoTinChi,HeSo)
               String payload = String.join(",", line);
               String message = "CSV," + payload; // Thêm tiền tố Nguồn

               channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
               System.out.println(" [CSV-MH] Đã gửi: '" + line[0] + "'");
           }
       }
       System.out.println("Hoàn tất gửi dữ liệu MonHoc từ CSV!");
   }
}
}

