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
package com.tkmtwo.kavro.serde;


import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;


/**
 *
 * @param <T> - a GenericContainer
 */
public final class SerDe<T extends GenericContainer> { 
  public static final byte MAGIC_BYTE = 0x0;


  //
  // Assumption...DatumReader will already have the schema since
  // we only use the specific API
  //
  public T deserialize(final byte[] payload, final DatumReader<T> reader) throws IOException {
    
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    
    //
    // Eat the magic byte
    //
    byte mb = buffer.get();
    
    
    //
    // Eat the schema id/fingerprint
    //
    byte[] fpBytes = new byte[64];
    buffer.get(fpBytes);
    
    String schemaId = new String(fpBytes);

    /* If we *were* to do schema resolution, it would look something like this:

    Schema schema = getSchemaResolver().getSchemaForId(schemaId);
    if (schema == null) {
      throw new IllegalArgumentException("Schema not found for id " + schemaId);
    }
    */



    //
    // The rest is the Avro message
    //
    int msgStart = buffer.position() + buffer.arrayOffset();
    int msgLength = buffer.limit() - 65;
    
    
    final Decoder decoder = DecoderFactory.get().binaryDecoder(payload, msgStart, msgLength, null);
    return reader.read(null, decoder);
  }
  
  
  
  
  
  public byte[] serialize(final T input, final DatumWriter<T> writer) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    
    //
    // First, the magic
    //
    out.write(MAGIC_BYTE);
    
    //
    // Second, the schema id (just the sha-256 fingerprint as a String)
    //
    String schemaId = fingerprint(input.getSchema());
    out.write(ByteBuffer.allocate(schemaId.length()).put(schemaId.getBytes()).array());
    
    //
    // Third, the Avro message
    //
    final Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(input, encoder);
    encoder.flush();
    
    return out.toByteArray();
  }
  
  
  
  
  private static String fingerprint(Schema s) {
    return Hashing
      .sha256()
      .hashString(SchemaNormalization.toParsingForm(s), Charsets.UTF_8)
      .toString();
  }

  
  
  
}
