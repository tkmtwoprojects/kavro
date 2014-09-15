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


import kafka.serializer.Encoder;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 *
 * @param <T> - a GenericContainer
 */
public class SpecificDatumEncoder<T extends GenericContainer>
  extends DatumSerDe<T> implements Encoder<T> {
  
  private final DatumWriter<T> datumWriter;
  
  public SpecificDatumEncoder(final Class<T> specificRecordClass) {
    datumWriter = new SpecificDatumWriter<T>(specificRecordClass);
  }
  
  @Override
    public byte[] toBytes(final T source) {
    return toBytes(source, datumWriter);
  }
}
