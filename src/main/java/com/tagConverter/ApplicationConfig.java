package com.tagConverter;


import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.clients.producer.ProducerConfig;

import org.mybatis.spring.annotation.MapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

@Configuration
public class ApplicationConfig {
    private static final Logger logger = LoggerFactory.getLogger(ApplicationConfig.class);
    private Properties consumerConfig = new Properties();
    private Properties producerConfig = new Properties();
    private DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
    private List<String> plantCodes;
    private String orgTopic;
    private String stdTopic;
    private String errTopic;
    private String noMasterTopic;
    private int threadCnt;
    public ApplicationConfig() throws IOException, ConfigException {
        build();
    }

    /**
     * ApplicationConfig 설정 초기화 함수
     * @param
     * @return
     * @throws IOException
     */
    private void build() throws ConfigException {
        File f = new File(getParentDirectoryFromJar());
        String filePath = f.separator + f.toString() + f.separator + "config"+ f.separator + "application.properties";
        Properties Prop = new Properties();
        try (FileInputStream fis = new FileInputStream(filePath)) {
            Prop.load(fis);
        } catch (IOException e) {
            throw new ConfigException("Error reading config file" + filePath ,e);
        } catch (Exception e) {
            throw new ConfigException("Error processing " + filePath ,e);
        }
        
        for (Entry<Object, Object> entry : Prop.entrySet()) {
            String key = entry.getKey().toString().trim();
            String value = entry.getValue().toString().trim();
            switch (key) {
                case "bootstrap.servers":
                    consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, value);
                    producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, value);
                    break;
                case "linger.ms":
                    producerConfig.setProperty(ProducerConfig.LINGER_MS_CONFIG,value);
                    break;
                case "batch.size":
                    producerConfig.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,value);
                    break;
                case "compression.type":
                    producerConfig.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,value);
                    break;
                case "buffer.memory":
                    producerConfig.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG,value);
                    break;
                case "acks":
                    producerConfig.setProperty(ProducerConfig.ACKS_CONFIG,value);
                    break;
                case "enable.auto.commit":
                    consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,value);
                    break;
                case "auto.commit.enable":
                    consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,value);
                    break;
                case "fetch.min.bytes":
                    consumerConfig.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,value);
                    break;
                case "fetch.max.wait.ms":
                    consumerConfig.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,value);
                    break;
                case "group.id":
                    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, value);
                    break;
                case "key.deserializer":
                    consumerConfig.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, value);
                    break;
                case "value.deserializer":
                    consumerConfig.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, value);
                    break;
                case "max.poll.records":
                    consumerConfig.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, value);
                    break;
                case "key.serializer":
                    producerConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, value);
                    break;
                case "value.serializer":
                    producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, value);
                    break;
                case "spring.datasource.driver-class-name":
                    dataSourceBuilder.driverClassName(value);
                    break;
                case "spring.datasource.url":
                    dataSourceBuilder.url(value);
                    break;
                case "spring.datasource.username":
                    dataSourceBuilder.username(value);
                    break;
                case "spring.datasource.password":
                    dataSourceBuilder.password(value);
                    break;
                case "converter.threadCnt":
                    threadCnt=Integer.parseInt(value);
                    break;
                case "converter.org.topic":
                    orgTopic=value;
                    break;
                case "converter.std.topic":
                    stdTopic=value;
                    break;
                case "converter.error.topic":
                    errTopic=value;
                    break;
                case "converter.nomaster.topic":
                    noMasterTopic=value;
                    break;
                case "converter.plantcodes":
                    String[] temp = value.split(",");
                    plantCodes = Arrays.asList(temp);
                    break;
            }
            logger.info("Task config setting : " + entry.toString());
        }
    }
    @Bean
    public DataSource dataSource() { return dataSourceBuilder.build(); }
    public Properties getConsumerConfig() {
        return consumerConfig;
    }
    public Properties getProducerConfig() {
        return producerConfig;
    }
    public List<String> getPlantCodes() {
        return plantCodes;
    }
    public String getNoMasterTopic(){ return noMasterTopic; }
    public String getErrTopic() { return errTopic; }
    public String getOrgTopic() {
        return orgTopic;
    }
    public String getStdTopic() {
        return stdTopic;
    }
    public int getThreadCnt() {
        return threadCnt;
    }
    public  String getParentDirectoryFromJar() {
        // String dirtyPath = ApplicationConfig.class.getResource("").toString();

        URL resourceUrl = ApplicationConfig.class.getResource("");
        if (resourceUrl == null) {
            throw new IllegalStateException("Resource path not found");
        }
        String dirtyPath = resourceUrl.toString();
        
        String jarPath = dirtyPath.replaceAll("^.*file:/", ""); //removes file:/ and everything before it
        jarPath = jarPath.replaceAll("jar!.*", "jar"); //removes everything after .jar, if .jar exists in dirtyPath
        jarPath = jarPath.replaceAll("%20", " "); //necessary if path has spaces within
        if (!jarPath.endsWith(".jar")) { // this is needed if you plan to run the app using Spring Tools Suit play button.
            jarPath = jarPath.replaceAll("/classes/.*", "/classes/");
        }
        logger.info("jarPath :" + jarPath);
        String directoryPath = Paths.get(jarPath).getParent().toString(); //Paths - from java 8
        logger.info("directoryPath :" + directoryPath);
        return directoryPath;
    }
    @SuppressWarnings("serial")
    public static class ConfigException extends Exception {
        public ConfigException(String msg, Exception e) {
            super(msg, e);
        }

    }
}
