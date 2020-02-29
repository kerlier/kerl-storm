package com.fashion.storm.trident.pojo;

import java.io.Serializable;

/**
 * 这个类存放的是疾病 发生的时间和地点
 *
 */
public class DiagnosisEvent implements Serializable {

    private Double lat;
    private Double lng;
    private Long time;
    private String diagnosisCode;// 这里的code使用的是ICD-9-CM中的编码

    public DiagnosisEvent() {
    }

    public DiagnosisEvent(Double lat, Double lng, Long time, String diagnosisCode) {
        this.lat = lat;
        this.lng = lng;
        this.time = time;
        this.diagnosisCode = diagnosisCode;
    }

    public Double getLat() {
        return lat;
    }

    public void setLat(Double lat) {
        this.lat = lat;
    }

    public Double getLng() {
        return lng;
    }

    public void setLng(Double lng) {
        this.lng = lng;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getDiagnosisCode() {
        return diagnosisCode;
    }

    public void setDiagnosisCode(String diagnosisCode) {
        this.diagnosisCode = diagnosisCode;
    }
}
