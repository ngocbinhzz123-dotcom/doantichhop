package com.doan.util;

// File này hứng TẤT CẢ các rule có thể có
public class FieldRules {
    private boolean checkNull;
    private boolean checkDuplicate;
    private boolean checkForeignKey; // Cho Lớp HP, Điểm
    private int maxLength;           // Cho Tên SV
    private boolean isInteger;       // Cho Số Tín Chỉ
    private boolean isDouble;        // Cho Hệ Số
    private double min = Double.NEGATIVE_INFINITY; // Cho Điểm
    private double max = Double.POSITIVE_INFINITY; // Cho Điểm
    private String regex;            // Cho Ngày sinh, Điểm

    // [THÊM] Rule đặc biệt cho Bảng Điểm
    private boolean checkDuplicatePair; 

    
    // --- Getters (Bắt buộc phải có) ---
    
    public boolean isCheckNull() { return checkNull; }
    public boolean isCheckDuplicate() { return checkDuplicate; }
    public boolean isCheckForeignKey() { return checkForeignKey; }
    public int getMaxLength() { return maxLength; }
    public boolean isInteger() { return isInteger; }
    public boolean isDouble() { return isDouble; }
    public double getMin() { return min; }
    public double getMax() { return max; }
    public String getRegex() { return regex; }
    
    // [THÊM] Getter cho rule mới
    public boolean isCheckDuplicatePair() { return checkDuplicatePair; }
}