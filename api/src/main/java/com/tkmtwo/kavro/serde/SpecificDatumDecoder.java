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


import kafka.serializer.Decoder;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;


/**
 *
 * @param <T> - a GenericContainer
 */
public class SpecificDatumDecoder<T extends GenericContainer>
  extends DatumSerDe<T> implements Decoder<T> {
  
  private final DatumReader<T> datumReader;
  
  public SpecificDatumDecoder(final Class<T> specificRecordBase) {
    datumReader = new SpecificDatumReader<T>(specificRecordBase);
  }
  
  @Override
    public T fromBytes(final byte[] bytes) {
    return fromBytes(bytes, datumReader);
  }
  
}
