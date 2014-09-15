package com.tkmtwo.kavro.serde;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import com.tkmtwo.kavro.Greeting;

import org.junit.Before;
import org.junit.Test;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.Schema;
import java.util.Properties;

public class SerDeTest {


  @Test
  public void testSerDe() {
    Greeting greetIn = 
      Greeting.newBuilder()
      .setFrom("Me")
      .setTo("Self")
      .setInstant(73L)
      .setMessage("Hello, self")
      .build();

    SpecificDatumEncoder<Greeting> greetEncoder =
      new SpecificDatumEncoder<Greeting>(Greeting.class);
    byte[] ba = greetEncoder.toBytes(greetIn);

    
    
    SpecificDatumDecoder<Greeting> greetDecoder =
      new SpecificDatumDecoder<Greeting>(Greeting.class);
    
    Greeting greetOut = greetDecoder.fromBytes(ba);



    assertEquals(greetIn.toString(), greetOut.toString());
    assertEquals(greetIn, greetOut);

  }
    
  
}
