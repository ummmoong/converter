package com.tagConverter.support;

import com.tagConverter.mapper.Tag;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import static com.tagConverter.support.RecordType.*;

/**
 * 데이터 검증 클래스
 * @author 스마트안전사업부 봉민석
 *
 */
@Component
public class RecordVerify {
    private final List<String> requiredKeys = Arrays.asList("ColTime", "OriTime", "Val", "Qual");
    private final Pattern TIME_PATTERN_NOT_DECIMAL_POINT = Pattern.compile("^\\d{4}(0[1-9]|1[012])(0[1-9]|[12]\\d|3[01])([01]\\d|2[0-3])([0-5]\\d)([0-5]\\d)$");
    private final Pattern TIME_PATTERN_DECIMAL_POINT = Pattern.compile("^\\d{4}(0[1-9]|1[012])(0[1-9]|[12]\\d|3[01])([01]\\d|2[0-3])([0-5]\\d)([0-5]\\d)\\.\\d{3}$");
    private final Pattern VAL_PATTERN = Pattern.compile("^(?:-?\\d+|-?\\d+\\.\\d+|-?\\d+(?:\\.\\d+)?[eE][-+]?\\d+)$");

    /**
     * Org 레코드의 Time 형식 검사 함수
     * @param time
     * @return boolean
     */
    private boolean VerifyTimeFormat(String time) {
        if (TIME_PATTERN_DECIMAL_POINT.matcher(time).matches()) {
            return true;
        }
        if(TIME_PATTERN_NOT_DECIMAL_POINT.matcher(time).matches()){
            return true;
        }
        return false;
    }

    /**
     * Org 레코드의 key, value 검사
     * @param key
     * @param value
     * @return boolean
     */
    private boolean VerifyRecordEmpty(String key,String value) {
        if(key == null || key.isEmpty()){
            return false;
        }
        if(value == null || value.isEmpty()){
            return false;
        }
        return true;
    }
    /**
     * record의 value 값을 검사하는 함수
     * 필요한 json 키 값의 존재 여부, json 타입의 value인지 검사
     * @param value
     * @return boolean
     */
    private boolean VerifyValueFormat(String value) {
        Object parsedValue = JSONValue.parse(value);
        if (parsedValue instanceof JSONObject) {
            JSONObject jsonValue = (JSONObject) parsedValue;
            if (jsonValue.keySet().containsAll(requiredKeys)) {
                return true;
            }
        }
        return false;
    }
    /**
     * 아날로그 태그의 Val 값을 검사
     * @param key
     * @param Val
     * @param tagMap
     * return boolean
     */
    private boolean VerifyAnalogTag(String key,String Val, HashMap<String, List<Tag>> tagMap){
        //아날로그 태그인 경우
        if(tagMap.containsKey(key)){
            if(tagMap.get(key).get(0).getDataType() == 1) {
                return VAL_PATTERN.matcher(Val.trim()).matches() ? true : false;
            }
        }
        //디지털 태그인 경우 Val 검사 스킵
        return true;
    }
    /**
     * Json의 Value의 형식 조사
     * @Param value
     * return boolean
     */
    private boolean VerifyValueString(String value){
        JSONObject jsonValue = (JSONObject) JSONValue.parse(value);
        for (Object key : jsonValue.keySet()) {
            Object object = jsonValue.get(key);
            if (!(object instanceof String)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 레코드를 검사하는 최종 함수
     * 1.메타데이터 검사
     * 2.VerifyRecordEmpty()
     * 3.VerifyValueFormat()
     * 4.VerifyTimeFormat()
     * 5.VerifyAnalogTag()
     * @param record
     * @param tagMap
     * @return RecordType
     */
    public RecordType isValid(ConsumerRecord<String,String> record,HashMap<String, List<Tag>> tagMap){
        String key = record.key();
        String value = record.value();

        //레코드 Empty 검사
        if(!VerifyRecordEmpty(key,value)){
            return ERROR;
        }
        //Tag명 따움표 검사
        if(key.contains("\"")) {
            return ERROR;
        }
        //value의 json key값, json 형식 검사
        if(!VerifyValueFormat(value)){
            return ERROR;
        }
        //레코드 Value가 String 타입인지 조사
        if(!VerifyValueString(value)){
            return ERROR;
        }
        //날짜 형식 검사
        Object parsedValue = JSONValue.parse(value);
        JSONObject jsonValue = (JSONObject) parsedValue;
        String OriTime = (String) jsonValue.get("OriTime");
        String ColTime = (String) jsonValue.get("ColTime");

        if(!VerifyTimeFormat(OriTime)){
            return ERROR;
        }
        if(!VerifyTimeFormat(ColTime)){
            return ERROR;
        }
        //메타데이터 검사
        if (!tagMap.containsKey(record.key())) {
            return NOMASTER;
        }
        //아날로그 태그 Val 검사
        String Val = (String) jsonValue.get("Val");
        if(!VerifyAnalogTag(key,Val,tagMap)){
            return ERROR;
        }

        //모든 레코드 검사를 통과
        return VALID;
    }


}
