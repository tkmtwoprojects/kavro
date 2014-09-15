package com.tkmtwo.kavro.producer;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

//import aisp.avro.RawHostEvent;  
import com.tkmtwo.kavro.Greeting;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.Schema;
import java.util.Properties;


import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.beans.factory.annotation.Autowired;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class KavroProducerTest {

  @Autowired
  KavroProducer<Greeting> kavroProducer;
  
  private Greeting newGreeting(int i) {
    long someInstant = System.currentTimeMillis();

    Greeting greeting = 
      Greeting.newBuilder()
      .setFrom("Me")
      .setTo("Self")
      .setInstant(System.currentTimeMillis())
      .setMessage(String.format("Hello, self: %d", i))
      .build();
    return greeting;
  }
  
  
  
  

  @Test
  public void testThis() {
    for (int i = 0; i < 5; i++) {
      kavroProducer.send(newGreeting(i));
    }
  }
  
  
}
