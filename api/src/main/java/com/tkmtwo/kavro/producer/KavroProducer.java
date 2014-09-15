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
package com.tkmtwo.kavro.producer;

import com.tkmtwo.kavro.serde.SpecificDatumEncoder;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
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
 * @param <T> - a GenericRecord
 */
public final class KavroProducer<T extends GenericContainer>
  implements InitializingBean, DisposableBean, FactoryBean {
  
  private final Logger logger = LoggerFactory.getLogger(KavroProducer.class);
  
  private Producer<Integer, byte[]> producer;
  private Properties properties = new Properties();
  
  private Class<T> datumClass;
  private SpecificDatumEncoder<T> datumEncoder;
  
  public KavroProducer(final Class<T> dc) {
    datumClass = dc;
  }
  
  public Class<T> getDatumClass() { return datumClass; }
  public void setDatumClass(Class<T> c) { datumClass = c; }
  
  private SpecificDatumEncoder<T> getDatumEncoder() {
    if (datumEncoder == null) {
      datumEncoder = new SpecificDatumEncoder<T>(getDatumClass());
    }
    return datumEncoder;
  }
  
  
  
  
  public void setProducer(Producer<Integer, byte[]> p) { producer = p; }
  public Producer<Integer, byte[]> getProducer() {
    if (producer == null) {
      producer = new Producer<Integer, byte[]>(new ProducerConfig(getProperties()));
    }
    return producer;
  }
  
  public void setProperties(Properties props) { properties = props; }
  public Properties getProperties() {
    if (properties == null) {
      properties = new Properties();
    }
    return properties;
  }

  private String getTopicName(T t) {
    //return t.getSchema().getName();
    //TODO(tkmtwo): assuming schema is a record|enum|fixed and has a namespace...
    return String.format("%s.%s", t.getSchema().getNamespace(), t.getSchema().getName());
  }
  public void send(T t) {
    send(getTopicName(t), getDatumEncoder().toBytes(t));
  }
  public void send(String topicName, byte[] bytes) {
    producer.send(new KeyedMessage<Integer, byte[]>(topicName, bytes));
  }
    
  
  /*
  public void send(GenericContainer input) {
    SpecificDatumEncoder sde = new SpecificDatumEncoder(input.getClass());
    byte[] bs = sde.toBytes(input);
  }
  
  protected void send(String topicName, byte[] bytes) {
    producer.send(new KeyedMessage<Integer, byte[]>(topicName, bytes));
  }
  */
    
    
  /*    
  public void send(String topicName, IndexedRecord indexedRecord) {
    byte[] bytes = kaEncoder.toBytes(indexedRecord);
    producer.send(new KeyedMessage<Integer, byte[]>(topicName, bytes));
  }
  */
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  private void checkForProperty(String s) {
    Assert.isTrue(StringUtils.hasText(getProperties().getProperty(s)), "Need a property for " + s);
  }
  
  /**
   * @see InitializingBean#afterPropertiesSet()
   */
  @Override
  public void afterPropertiesSet() {
    checkForProperty("serializer.class");
    checkForProperty("metadata.broker.list");
    checkForProperty("request.timeout.ms");
    checkForProperty("timeout.ms");
    checkForProperty("message.send.max.retries");


    //datumEncoder = new SpecificDatumEncoder<T>(RawHostEvent.class);
    
    
    //Go ahead and create the producer
    getProducer();
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
    return KavroProducer.class;
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
  }
  
  
}

