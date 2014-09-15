package com.tkmtwo.kavro.consumer;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.Schema;
import java.util.Properties;
//import aisp.avro.RawHostEvent;  
import com.tkmtwo.kavro.Greeting;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.beans.factory.annotation.Autowired;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class KavroConsumerTest {


  @Autowired
  private KavroConsumer kavroConsumer;

  
  
  @Test
  public void testThis() throws Exception {
    kavroConsumer.run();
    Thread.sleep(3000L);
  }
  
  
  
  
}
