package com.doan.luonglophocphan;

import com.doan.util.ketnoi;
import com.doan.util.FieldRules;
import com.doan.util.LopHocPhanFiledRules;
import com.google.gson.Gson;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.regex.Pattern;

public class LopHocPhanCR {

    private static final String SQL_SELECT_STAGING = "SELECT * FROM STAGING_LOPHOCPHAN WHERE TrangThaiQC = 'PENDING'";
    private static final String SQL_INSERT_TARGET = "INSERT INTO LOPHOCPHAN_TBL (MaLopHP, MaMon, MaGV, HocKy, NamHoc) VALUES (?, ?, ?, ?, ?)";
    private static final String SQL_UPDATE_STAGING = "UPDATE STAGING_LOPHOCPHAN SET TrangThaiQC = ? WHERE StagingID = ?";
    private static final String SQL_LOG_ERROR = "INSERT INTO QC_LOG_TBL (StagingID, RuleName, ErrorMessage) VALUES (?, ?, ?)";
    
    private static LopHocPhanFiledRules cauHinhRule;
    private static int hocKy_DaLamSach;
    private static int namHoc_DaLamSach;

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
                            kiemTraHopLe_LopHocPhan(conn, ketQuaStaging); 
                            
                            try (PreparedStatement psInsert = conn.prepareStatement(SQL_INSERT_TARGET)) {
                                psInsert.setString(1, ketQuaStaging.getString("MaLopHP"));
                                psInsert.setString(2, ketQuaStaging.getString("MaMon"));
                                psInsert.setString(3, ketQuaStaging.getString("MaGV"));
                                psInsert.setInt(4, hocKy_DaLamSach); 
                                psInsert.setInt(5, namHoc_DaLamSach); 
                                psInsert.executeUpdate();
                            }
                            capNhatTrangThaiStaging(conn, stagingId, "PASSED");
                            System.out.println("PASSED [LopHocPhan]: StagingID " + stagingId);
                        } catch (Exception loi) {
                            System.err.println("FAILED [LopHocPhan]: StagingID " + stagingId + " - " + loi.getMessage());
                            ghiNhatKyLoi(conn, stagingId, "LopHocPhanCheckRule", loi.getMessage());
                            capNhatTrangThaiStaging(conn, stagingId, "FAILED");
                        }
                    }
                }
                conn.commit(); 
                System.out.println("Hoan tat kiem tra chat luong [LopHocPhan].");
            }
        } catch (Exception e) { e.printStackTrace(); }
    }
    
    private static void taiCauHinhRule() throws Exception {
        Gson gson = new Gson();
        InputStream is = LopHocPhanCR.class.getClassLoader().getResourceAsStream("lophocphan_rules.json");
        if (is == null) throw new Exception("Loi: Khong tim thay file 'lophocphan_rules.json'");
        Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
        cauHinhRule = gson.fromJson(reader, LopHocPhanFiledRules.class);
        System.out.println("Da load file config rule [LopHocPhan] thanh cong.");
    }
    
    private static void kiemTraHopLe_LopHocPhan(Connection conn, ResultSet rs) throws Exception {
        FieldRules rule_MaLopHP = cauHinhRule.getMaLopHP();
        FieldRules rule_MaMon = cauHinhRule.getMaMon();
        FieldRules rule_MaGV = cauHinhRule.getMaGV();
        FieldRules rule_HocKy = cauHinhRule.getHocKy();
        FieldRules rule_NamHoc = cauHinhRule.getNamHoc();
        
        String maLopHP = rs.getString("MaLopHP");
        String maMon = rs.getString("MaMon");
        String maGV = rs.getString("MaGV");
        String hocKyStr = rs.getString("HocKy");
        String namHocStr = rs.getString("NamHoc");
        
        // Check MaLopHP
        if (rule_MaLopHP.isCheckNull() && (maLopHP == null || maLopHP.isEmpty())) throw new Exception("Loi: MaLopHP bi rong (NULL).");
        if (rule_MaLopHP.isCheckDuplicate() && kiemTraMaLopHPDaTonTai(conn, maLopHP)) throw new Exception("Loi: MaLopHP '" + maLopHP + "' da ton tai.");
        if (rule_MaLopHP.getRegex() != null && !Pattern.matches(rule_MaLopHP.getRegex(), maLopHP)) throw new Exception("Loi Regex: MaLopHP '" + maLopHP + "' khong dung dinh dang (vi du: LHP001).");
        
        // Check MaMon
        if (rule_MaMon.isCheckNull() && (maMon == null || maMon.isEmpty())) throw new Exception("Loi: MaMon bi rong (NULL).");
        if (rule_MaMon.isCheckForeignKey() && !kiemTraMaMonDaTonTai(conn, maMon)) throw new Exception("Loi: MaMon '" + maMon + "' khong ton tai.");
        
        // Check MaGV
        if (rule_MaGV.isCheckNull() && (maGV == null || maGV.isEmpty())) throw new Exception("Loi: MaGV bi rong (NULL).");
        if (rule_MaGV.isCheckForeignKey() && !kiemTraMaGvDaTonTai(conn, maGV)) throw new Exception("Loi: MaGV '" + maGV + "' khong ton tai.");
        
        // Check HocKy
        if (rule_HocKy.isCheckNull() && (hocKyStr == null || hocKyStr.isEmpty())) throw new Exception("Loi: HocKy bi rong (NULL).");
        if (rule_HocKy.getRegex() != null && !Pattern.matches(rule_HocKy.getRegex(), hocKyStr)) throw new Exception("Loi Regex: HocKy '" + hocKyStr + "' khong hop le (phai la 1, 2, hoac 3).");
        hocKy_DaLamSach = Integer.parseInt(hocKyStr);
        
        // Check NamHoc
        if (rule_NamHoc.isCheckNull() && (namHocStr == null || namHocStr.isEmpty())) throw new Exception("Loi: NamHoc bi rong (NULL).");
        if (rule_NamHoc.getRegex() != null && !Pattern.matches(rule_NamHoc.getRegex(), namHocStr)) throw new Exception("Loi Regex: NamHoc '" + namHocStr + "' khong hop le (phai la 4 chu so, bat dau 20xx).");
        namHoc_DaLamSach = Integer.parseInt(namHocStr);
    }
    
    private static boolean kiemTraMaLopHPDaTonTai(Connection conn, String maLopHP) throws SQLException {
        String sql = "SELECT 1 FROM LOPHOCPHAN_TBL WHERE MaLopHP = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, maLopHP);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }
    private static boolean kiemTraMaMonDaTonTai(Connection conn, String maMon) throws SQLException {
        String sql = "SELECT 1 FROM MON_HOC_TBL WHERE MaMon = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, maMon);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }
    private static boolean kiemTraMaGvDaTonTai(Connection conn, String maGV) throws SQLException {
        String sql = "SELECT 1 FROM GIANG_VIEN_TBL WHERE MaGV = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, maGV);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
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