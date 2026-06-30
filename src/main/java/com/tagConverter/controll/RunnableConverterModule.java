package com.tagConverter.controll;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tagConverter.ApplicationConfig;
import com.tagConverter.mapper.Tag;
import com.tagConverter.support.RecordConvert;
import com.tagConverter.support.RecordVerify;


/**
 * 데이터 검증 및 표준화 로직
 * @author 스마트안전사업부 봉민석
 *
 */
public class RunnableConverterModule implements Runnable{
    private RecordVerify recordVerify;
    private RecordConvert recordConvert;
    private HashMap<String, List<Tag>> tagMap;
    private ApplicationConfig config;
    private static AtomicInteger processCnt = new AtomicInteger(0);
    private static AtomicInteger errorCnt = new AtomicInteger(0);
    private static AtomicBoolean isRunning = new AtomicBoolean(true);
    private static Set threadInfo = new HashSet();
    private final Logger logger = LoggerFactory.getLogger(RunnableConverterModule.class);
    /**
     * 변수를 초기화 하는 RunnableConverterModule 클래스의 생성자
     * @param tm
     * @param cf
     * @param rv
     * @param rc
     */
    public RunnableConverterModule(HashMap<String, List<Tag>> tm, ApplicationConfig cf, RecordVerify rv, RecordConvert rc){
            recordConvert = rc;
            recordVerify = rv;
            config = cf;
            tagMap = tm;
    }
    /**
     * 컨버터 실행 로직
     * isValld() 함수를 통해 유효한 레코드 인지 판별
     * doConvert() 함수를 통해서 태그 표준화 수행
     * doCo
     * graceful shutdown 제공
     * @param
     * @return void
     */
    @Override
    public void run() {
        //Topic 설정
        String orgTopic = config.getOrgTopic();
        String stdTopic = config.getStdTopic();
        String errTopic = config.getErrTopic();
        String nomasterTopic = config.getNoMasterTopic();

        //producer, consumer 설정
        KafkaProducer<String, String> producer = new KafkaProducer<>(config.getProducerConfig());
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(config.getConsumerConfig());
        consumer.subscribe(Collections.singleton(orgTopic));

        //log를 위한 변수 초기화
        int processTempCnt = 0;
        int errorTempCnt = 0;

        //commit을 위한 currentOffsets 선언
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        //쓰레드 등록
        threadInfo.add(Thread.currentThread().getName());
        logger.info("{} Task start",Thread.currentThread().getName());
        //데이터 처리 로직
            while (isRunning.get()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(300);
                    for (ConsumerRecord<String, String> record : records) {
                        switch (recordVerify.isValid(record, tagMap)) {
                            case VALID:
                                List<Tag> tmp = tagMap.get(record.key());
                                //dcs_tag가 중복인 tag를 처리하기 위해 for문 사용
                                for (Tag tag : tmp) {
                                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                                            stdTopic,
                                            tag.getOrgTag(),
                                            recordConvert.doConvert(record, tag)
                                    );
                                    producer.send(producerRecord);
                                    processTempCnt++;
                                }
                                break;
                            case NOMASTER:
                                ProducerRecord<String, String> noMasterRecord = new ProducerRecord<>(
                                        nomasterTopic,
                                        record.key(),
                                        recordConvert.doConvertNoMaster(record)
                                );
                                producer.send(noMasterRecord);
                                processTempCnt++;
                                break;
                            case ERROR:
                                ProducerRecord<String, String> errorRecord = new ProducerRecord<>(
                                        errTopic,
                                        record.key() == null ? "null" : record.key(),
                                        record.value()
                                );
                                producer.send(errorRecord);
                                errorTempCnt++;
                                break;
                        }
                        currentOffsets.put(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1, null)
                        );
                    }
                    IncrementInteger(processTempCnt, errorTempCnt);
                    processTempCnt = errorTempCnt = 0;
                    consumer.commitAsync(currentOffsets, null);

                }catch(RuntimeException e){
                    logger.error(e.getMessage());
                } catch(Exception e){
                    logger.error(e.getMessage());
                }
        }
        //graceful shutdown을 위한 producer,consumer 종료
        producer.flush();
        producer.close();
        consumer.commitSync(currentOffsets);
        consumer.close();
        logger.info("{} Task Complete",Thread.currentThread().getName());
        threadInfo.remove(Thread.currentThread().getName());
    }
    public static void shutdown(){
        isRunning.set(false);
    }
    public static Boolean getThreadInfo(){
        return threadInfo.isEmpty();
    }
    public static int getProcessCnt(){
        return processCnt.get();
    }
    public static int getErrorCnt(){
        return errorCnt.get();
    }
    private void IncrementInteger(int processTempCnt, int errorTempCnt){
        processCnt.addAndGet(processTempCnt);
        errorCnt.addAndGet(errorTempCnt);
    }
    public static void cntInit(){
        errorCnt.set(0);
        processCnt.set(0);
    }
}
