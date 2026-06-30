package com.tagConverter.support;

/**
 * RecordVerify를 통해 지정될 레코드의 타입
 * ERROR : Org 레코드가 유효하지 않을 경우
 * VALID : Org 레코드가 유효한 경우
 * NOMASTER : Org 레코드의 마스터 정보가 등록이 안된 경우
 */
public enum RecordType {
    ERROR,VALID,NOMASTER
}
