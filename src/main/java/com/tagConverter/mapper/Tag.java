package com.tagConverter.mapper;


public class Tag {
    private String plantCode;
    private String orgTag;
    private String stdTag;
    private String dcsTag;
    private int dataType;
    public int getDataType() { return dataType; }
    public String getPlantCode() {
        return plantCode;
    }
    public String getOrgTag() { return orgTag; }
    public String getStdTag() {
        return (stdTag == null || stdTag.isEmpty()) ? orgTag : stdTag;
    }
    public boolean hasDcsData(){ return dcsTag != null && !dcsTag.isEmpty(); }
    public String getDcsTag() { return dcsTag; }

}
