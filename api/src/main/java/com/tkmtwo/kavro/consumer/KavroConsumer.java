/*
 *
 * Copyright 2014 Tom Mahaffey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.tkmtwo.kavro.consumer;
 
import com.tkmtwo.kavro.serde.SpecificDatumDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.avro.generic.GenericContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 *
 * @param <T> - a GenericContainer
 */ 
public abstract class KavroConsumer<T extends GenericContainer>
  implements DisposableBean, FactoryBean, InitializingBean {
  
  //protected final Logger logger = LoggerFactory.getLogger(KavroConsumer.class);
  protected final Logger logger = LoggerFactory.getLogger(getClass());

  private ConsumerConnector consumerConnector;
  private String topicName;
  private ExecutorService executor;
  private Properties properties;
  private int concurrency = 1;
  
  
  private Class<T> datumClass;
  //private SpecificDatumEncoder<T> datumEncoder;
  
  
  
  
  public KavroConsumer(final Class<T> dc) {
    datumClass = dc;
  }
  
  
  private ConsumerConnector getConsumerConnector() { return consumerConnector; }
  private void setConsumerConnector(ConsumerConnector cc) { consumerConnector = cc; }
  
  public String getTopicName() { return topicName; }
  public void setTopicName(String s) { topicName = s; }
  
  public Properties getProperties() { return properties; }
  public void setProperties(Properties p) { properties = p; }
  
  private ExecutorService getExecutor() { return executor; }
  private void setExecutor(ExecutorService es) { executor = es; }
  
  public int getConcurrency() {
    if (concurrency <= 0) {
      concurrency = 1;
    }
    return concurrency;
  }
  
  public void setConcurrency(int i) { concurrency = i; }
  

  public Class<T> getDatumClass() { return datumClass; }
  public void setDatumClass(Class<T> c) { datumClass = c; }
  
  
  
  
  
  public void run() {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(getTopicName(), new Integer(getConcurrency()));
    
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
      getConsumerConnector().createMessageStreams(topicCountMap);
    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(getTopicName());
    
    // now launch all the threads
    //
    logger.info("Launching stream-listening threads.");
    setExecutor(Executors.newFixedThreadPool(getConcurrency()));

    
    // now create an object to consume the messages
    //
    for (final KafkaStream<byte[], byte[]> stream : streams) {
      getExecutor().submit(new ReaderRunner(stream));
    }
    
    
  }
  
  
  private class ReaderRunner implements Runnable {
    private KafkaStream<byte[], byte[]> kafkaStream;
    private SpecificDatumDecoder<T> datumDecoder;
    
    public ReaderRunner(KafkaStream<byte[], byte[]> ks) {
      setKafkaStream(ks);
      setDatumDecoder(new SpecificDatumDecoder<T>(getDatumClass()));
    }
    
    private KafkaStream<byte[], byte[]> getKafkaStream() { return kafkaStream; }
    private void setKafkaStream(KafkaStream<byte[], byte[]> ks) { kafkaStream = ks; }
    
    private SpecificDatumDecoder<T> getDatumDecoder() { return datumDecoder; }
    private void setDatumDecoder(SpecificDatumDecoder<T> sdd) { datumDecoder = sdd; }
    
    
    public void run() {
      ConsumerIterator<byte[], byte[]> it = getKafkaStream().iterator();
      while (it.hasNext()) {
        byte[] pl = it.next().message();
        T specificRecord = getDatumDecoder().fromBytes(pl);
        doWithDatum(specificRecord);
      }
      
    }
    
  }
  
  
  
  
  protected abstract void doWithDatum(T t);

  
  private void checkForProperty(String s) {
    Assert.isTrue(StringUtils.hasText(getProperties().getProperty(s)), "Need a property for " + s);
  }

  /**
   * @see InitializingBean#afterPropertiesSet()
   */
  @Override
  public void afterPropertiesSet() {
    logger.info("KavroConsumer for {} coming up.", getTopicName());

    Assert.isTrue(StringUtils.hasText(getTopicName()), "Need a topic name.");
    checkForProperty("zookeeper.connect");
    checkForProperty("group.id");
    checkForProperty("zookeeper.session.timeout.ms");
    checkForProperty("zookeeper.sync.time.ms");
    checkForProperty("auto.commit.interval.ms");
    checkForProperty("auto.offset.reset");
    setConsumerConnector(Consumer.createJavaConsumerConnector(new ConsumerConfig(getProperties())));

    logger.info("KavroConsumer for {} is up.", getTopicName());
  }
  
  /**
   * @see FactoryBean#getObject()
   */
  @Override
  public Object getObject() {
    return this;
  }
  
  /**
   * @see FactoryBean#getObjectType()
   */
  @Override
  public Class getObjectType() {
    return KavroConsumer.class;
  }
  
  /**
   * @see FactoryBean#isSingleton()
   */
  @Override
  public boolean isSingleton() {
    return true;
  }
  
  /**
   * @see DisposableBean#destroy()
   */
  @Override
  public void destroy() {
    logger.info("KavroConsumer for {} going down.", getTopicName());
    if (getConsumerConnector() != null) { getConsumerConnector().shutdown(); }
    if (getExecutor() != null) { getExecutor().shutdown(); }
  }

  
  
}
