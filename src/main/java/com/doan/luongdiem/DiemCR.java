package com.doan.luongdiem;

import com.doan.util.ketnoi;
import com.doan.util.DiemFieldRules; 
import com.doan.util.FieldRules;
import com.google.gson.Gson;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.regex.Pattern; 

public class DiemCR {

    private static final String SQL_SELECT_STAGING = "SELECT * FROM STAGING_DIEM WHERE TrangThaiQC = 'PENDING'";
    private static final String SQL_INSERT_TARGET = "INSERT INTO DIEM_TBL " +
        "(MaSV, MaLopHP, DiemPhatBieu, DiemBaiTap, DiemChuyenCan, DiemGiuaKy, DiemDoAn, DiemCuoiKi, DiemTongKet, DiemHe4, DiemChu) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String SQL_UPDATE_STAGING = "UPDATE STAGING_DIEM SET TrangThaiQC = ? WHERE StagingID = ?";
    private static final String SQL_LOG_ERROR = "INSERT INTO QC_LOG_TBL (StagingID, RuleName, ErrorMessage) VALUES (?, ?, ?)";
    
    private static final double TRONGSO_PHATBIEU = 0.10; 
    private static final double TRONGSO_BAITAP = 0.10;
    private static final double TRONGSO_CHUYENCAN = 0.05;
    private static final double TRONGSO_GIUAKY = 0.20;
    private static final double TRONGSO_DOAN = 0.10;
    private static final double TRONGSO_CUOIKI = 0.45; 
    
    private static DiemFieldRules cauHinhRule; 
    private static double[] diemDaLamSach = new double[6]; 

    public static void main(String[] args) {
        try {
            taiCauHinhRule();
            try (Connection conn = ketnoi.getConnectionDich()) { 
                conn.setAutoCommit(false); 
                try (Statement stmt = conn.createStatement();
                     ResultSet ketQuaStaging = stmt.executeQuery(SQL_SELECT_STAGING)) { 
                    while (ketQuaStaging.next()) {
                        int stagingId = ketQuaStaging.getInt("StagingID");
                        try {
                            kiemTraHopLe_Diem(conn, ketQuaStaging); 
                            
                            // --- TRANSFORM: Tính toán ---
                            double diemTongKet = (diemDaLamSach[0] * TRONGSO_PHATBIEU) +
                                                 (diemDaLamSach[1] * TRONGSO_BAITAP) +
                                                 (diemDaLamSach[2] * TRONGSO_CHUYENCAN) +
                                                 (diemDaLamSach[3] * TRONGSO_GIUAKY) +
                                                 (diemDaLamSach[4] * TRONGSO_DOAN) +
                                                 (diemDaLamSach[5] * TRONGSO_CUOIKI);
                            
                            diemTongKet = Math.round(diemTongKet * 10.0) / 10.0;
                            String diemChu = tinhDiemChu(diemTongKet);
                            double diemHe4 = tinhDiemHe4(diemTongKet);
                            
                            // --- XỬ LÝ KHI PASS ---
                            try (PreparedStatement psInsert = conn.prepareStatement(SQL_INSERT_TARGET)) {
                                psInsert.setString(1, ketQuaStaging.getString("MaSV"));
                                psInsert.setString(2, ketQuaStaging.getString("MaLopHP"));
                                psInsert.setDouble(3, diemDaLamSach[0]);
                                psInsert.setDouble(4, diemDaLamSach[1]);
                                psInsert.setDouble(5, diemDaLamSach[2]);
                                psInsert.setDouble(6, diemDaLamSach[3]);
                                psInsert.setDouble(7, diemDaLamSach[4]);
                                psInsert.setDouble(8, diemDaLamSach[5]);
                                psInsert.setDouble(9, diemTongKet); 
                                psInsert.setDouble(10, diemHe4);
                                psInsert.setString(11, diemChu);
                                psInsert.executeUpdate();
                            }
                            capNhatTrangThaiStaging(conn, stagingId, "PASSED");
                            System.out.println("PASSED [Diem]: StagingID " + stagingId);
                        } catch (Exception loi) {
                            // --- XỬ LÝ KHI FAILED ---
                            System.err.println("FAILED [Diem]: StagingID " + stagingId + " - " + loi.getMessage());
                            ghiNhatKyLoi(conn, stagingId, "DiemCheckRule", loi.getMessage());
                            capNhatTrangThaiStaging(conn, stagingId, "FAILED");
                        }
                    }
                }
                conn.commit(); 
                System.out.println("Hoan tat kiem tra chat luong [Diem].");
            }
        } catch (Exception e) { e.printStackTrace(); }
    }
    
    private static void taiCauHinhRule() throws Exception {
        Gson gson = new Gson();
        InputStream is = DiemCR.class.getClassLoader().getResourceAsStream("diem_rules.json");
        if (is == null) throw new Exception("Loi: Khong tim thay file 'diem_rules.json'");
        Reader reader = new InputStreamReader(is);
        cauHinhRule = gson.fromJson(reader, DiemFieldRules.class);
        System.out.println("Da load file config rule [Diem] thanh cong.");
    }
    
    private static void kiemTraHopLe_Diem(Connection conn, ResultSet rs) throws Exception {
        FieldRules rule_MaSV = cauHinhRule.getMaSV();
        FieldRules rule_MaLopHP = cauHinhRule.getMaLopHP();
        FieldRules rule_ChungChoDiem = cauHinhRule.getCheckDiem(); // Rule chung
        
        String maSV = rs.getString("MaSV");
        String maLopHP = rs.getString("MaLopHP");
        String[] diemTho = {
            rs.getString("DiemPhatBieu"), rs.getString("DiemBaiTap"),
            rs.getString("DiemChuyenCan"), rs.getString("DiemGiuaKy"),
            rs.getString("DiemDoAn"), rs.getString("DiemCuoiKi")
        };
        String[] tenCotDiem = {"DiemPhatBieu", "DiemBaiTap", "DiemChuyenCan", "DiemGiuaKy", "DiemDoAn", "DiemCuoiKi"};
        
        // Check MaSV
        if (rule_MaSV.isCheckNull() && (maSV == null || maSV.isEmpty())) throw new Exception("Loi: MaSV bi rong (NULL).");
        if (rule_MaSV.isCheckForeignKey() && !kiemTraMaSvDaTonTai(conn, maSV)) throw new Exception("Loi: MaSV '" + maSV + "' khong ton tai.");
        
        // Check MaLopHP
        if (rule_MaLopHP.isCheckNull() && (maLopHP == null || maLopHP.isEmpty())) throw new Exception("Loi: MaLopHP bi rong (NULL).");
        if (rule_MaLopHP.isCheckForeignKey() && !kiemTraMaLopHPDaTonTai(conn, maLopHP)) throw new Exception("Loi: MaLopHP '" + maLopHP + "' khong ton tai.");
        if (rule_MaLopHP.isCheckDuplicatePair() && kiemTraDiemDaTonTai(conn, maSV, maLopHP)) throw new Exception("Loi: Diem cho cap (SV, LHP) nay da ton tai.");
        
        // Check 6 cột điểm (Dùng 1 rule chung `CheckDiem`)
        for (int i = 0; i < diemTho.length; i++) {
            String val = diemTho[i];
            if (rule_ChungChoDiem.isCheckNull() && (val == null || val.isEmpty())) throw new Exception("Loi: " + tenCotDiem[i] + " bi rong (NULL).");
            if (rule_ChungChoDiem.getRegex() != null && !Pattern.matches(rule_ChungChoDiem.getRegex(), val)) throw new Exception("Loi Regex: " + tenCotDiem[i] + " '" + val + "' khong hop le.");
            
            try {
                double diem = Double.parseDouble(val);
                if (diem < rule_ChungChoDiem.getMin() || diem > rule_ChungChoDiem.getMax()) throw new Exception("Loi Range: " + tenCotDiem[i] + " '" + diem + "' phai trong [0, 10].");
                diemDaLamSach[i] = diem; // Lưu vào mảng sạch
            } catch (Exception e) {
                throw new Exception("Loi: " + tenCotDiem[i] + " '" + val + "' khong phai so.");
            }
        }
    }
    
    private static boolean kiemTraMaSvDaTonTai(Connection conn, String maSV) throws SQLException {
        String sql = "SELECT 1 FROM SINH_VIEN_TBL WHERE MaSV = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, maSV);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }
    private static boolean kiemTraMaLopHPDaTonTai(Connection conn, String maLopHP) throws SQLException {
        String sql = "SELECT 1 FROM LOPHOCPHAN_TBL WHERE MaLopHP = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, maLopHP);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }
    private static boolean kiemTraDiemDaTonTai(Connection conn, String maSV, String maLopHP) throws SQLException {
        String sql = "SELECT 1 FROM DIEM_TBL WHERE MaSV = ? AND MaLopHP = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, maSV);
            ps.setString(2, maLopHP);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }
    private static double tinhDiemHe4(double d) {
        if (d >= 8.5) return 4.0; if (d >= 8.0) return 3.7; if (d >= 7.0) return 3.0;
        if (d >= 6.5) return 2.5; if (d >= 5.5) return 2.0; if (d >= 5.0) return 1.5;
        if (d >= 4.0) return 1.0; return 0.0;
    }
    private static String tinhDiemChu(double d) {
        if (d >= 8.5) return "A"; if (d >= 8.0) return "B+"; if (d >= 7.0) return "B";
        if (d >= 6.5) return "C+"; if (d >= 5.5) return "C"; if (d >= 5.0) return "D+";
        if (d >= 4.0) return "D"; return "F";
    }
    private static void capNhatTrangThaiStaging(Connection conn, int stagingId, String status) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_UPDATE_STAGING)) {
            ps.setString(1, status);
            ps.setInt(2, stagingId);
            ps.executeUpdate();
        }
    }
    private static void ghiNhatKyLoi(Connection conn, int stagingId, String ruleName, String errorMessage) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LOG_ERROR)) {
            ps.setObject(1, stagingId, Types.INTEGER); 
            ps.setString(2, ruleName);
            ps.setString(3, errorMessage);
            ps.executeUpdate();
        }
    }
}