package com.tagConverter;


import java.util.ArrayList;
import java.util.HashMap;

import com.tagConverter.Service.ConverterDataService;
import com.tagConverter.controll.RunnableConverterModule;
import com.tagConverter.mapper.Tag;

import com.tagConverter.support.RecordConvert;
import com.tagConverter.support.RecordVerify;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;



import java.util.Map;
import java.util.concurrent.TimeUnit;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


/**
 * 태그 컨버터 메인 쓰레드
 * @author 스마트안전사업부 봉민석
 */
@SpringBootApplication
public class ConverterApplications {
    private static final Logger logger = LoggerFactory.getLogger(ConverterApplications.class);
    public static void main(String[] args){
        try (ConfigurableApplicationContext ctx = SpringApplication.run(ConverterApplications.class, args)) {
            //getBean을 통한 변수 초기화
            ApplicationConfig cf = ctx.getBean(ApplicationConfig.class);
            RecordVerify rv = ctx.getBean(RecordVerify.class);
            RecordConvert rc = ctx.getBean(RecordConvert.class);
            ConverterDataService cds = ctx.getBean(ConverterDataService.class);

            //db에서 Tag 정보 읽기
            List<Tag> tagList = cds.selectAllTags(cf.getPlantCodes());

            //tag map 생성
            //org_tag, dcs_tag 등록
            //dcs_tag가 중복인 tag를 처리하기 위해 List<Tag> 사용
            HashMap<String, List<Tag>> tagMap = new HashMap<>();
            for (Tag tag : tagList) {
                addValueToMap(tagMap,tag.getOrgTag(),tag);
                if(tag.hasDcsData()){
                    addValueToMap(tagMap,tag.getDcsTag(),tag);
                }
            }
            tagList.clear();

            //Converter Thread pool 생성 및 실행
            int mxTh = cf.getThreadCnt();
            ExecutorService ex = Executors.newFixedThreadPool(mxTh,runnable -> new Thread(runnable));
            for(int i =0; i <mxTh; i++) {
                ex.execute(new RunnableConverterModule(tagMap,cf,rv,rc));
            }

            //컨버터 모니터링 쓰레드 풀 생성 및 실행
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            Runnable monitoring = () -> {
                logger.info("{} Processing: {} , Error: {}"
                        ,cf.getPlantCodes().toString()
                        ,RunnableConverterModule.getProcessCnt()
                        ,RunnableConverterModule.getErrorCnt());
                RunnableConverterModule.cntInit();
            };
            scheduler.scheduleAtFixedRate(monitoring, 5, 10, TimeUnit.SECONDS);

            //인터럽트가 발생하면 셧다운 훅이 동작
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                scheduler.shutdownNow();
                RunnableConverterModule.shutdown();
                waitForThread();
            }));
        }catch (Exception e) {
            // TODO: handle exception
            logger.error(e.getMessage());
        }
    }
    private static void waitForThread() {
        while (!RunnableConverterModule.getThreadInfo()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
    private static void addValueToMap(Map<String, List<Tag>> map, String key, Tag value) {
        if (!map.containsKey(key)) {
            map.put(key, new ArrayList<>());
        }
        map.get(key).add(value);
    }

}
