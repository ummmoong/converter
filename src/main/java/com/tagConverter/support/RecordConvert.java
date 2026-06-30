package com.tagConverter.support;


import com.tagConverter.mapper.Tag;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 레코드 표준화 클래스
 * @author 스마트안전사업부 봉민석
 *
 */
@Component
public class RecordConvert {
    private static final Logger logger = LoggerFactory.getLogger(RecordConvert.class);

    /**
     * Org 레코드의 날짜 형식을 Std 레코드의 날짜 형식으로 변환
     * @param input
     * @return String
     */
    private String ChangeTimeFormat(String input)  {
        SimpleDateFormat beforeDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        SimpleDateFormat afterDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = beforeDateFormat.parse(input);
        } catch (ParseException e) {
            // TODO: handle exception
            logger.error(e.getMessage());
        }
        return afterDateFormat.format(date);
    }
    /**
     * 레코드 변환 함수
     * @param cr
     * @param tag
     * @return String
     */
    public String doConvert(ConsumerRecord<String, String> cr, Tag tag) {
        JSONObject orgJson = (JSONObject) JSONValue.parse(cr.value());
        return String.format("{\"Val\":\"%s\",\"SensorType\":\"%s\",\"std_tag\":\"%s\",\"OriTime\":\"%s\",\"ColTime\":\"%s\",\"Qual\":\"%s\",\"org_tag\":\"%s\",\"PlantCode\":\"%s\"}",
                ((String) orgJson.get("Val")).trim(),
                "null",
                tag.getStdTag(),
                ChangeTimeFormat((String) orgJson.get("OriTime")),
                ChangeTimeFormat((String) orgJson.get("ColTime")),
                orgJson.get("Qual"),
                tag.getOrgTag(),
                tag.getPlantCode()
        );
    }

    public String doConvertNoMaster(ConsumerRecord<String, String> cr) {
        JSONObject orgJson = (JSONObject) JSONValue.parse(cr.value());
        //메타데이터가 없는 경우
        return String.format("{\"Val\":\"%s\",\"SensorType\":\"%s\",\"std_tag\":\"%s\",\"OriTime\":\"%s\",\"ColTime\":\"%s\",\"Qual\":\"%s\",\"org_tag\":\"%s\",\"PlantCode\":\"%s\"}",
                ((String) orgJson.get("Val")).trim(),
                "null",
                cr.key(),
                ChangeTimeFormat((String) orgJson.get("OriTime")),
                ChangeTimeFormat((String) orgJson.get("ColTime")),
                orgJson.get("Qual"),
                cr.key(),
                "null"
        );
    }

}
