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


import java.io.IOException;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

/**
 *
 * @param <T> - a GenericContainer
 */
public abstract class DatumSerDe<T extends GenericContainer> {
  
  private final SerDe<T> serDe;
  
  protected DatumSerDe() {
    serDe = new SerDe<T>();
  }
  
  public byte[] toBytes(final T source, final DatumWriter<T> writer) {
    try {
      return serDe.serialize(source, writer);
    } catch (IOException ioex) {
      throw new SerDeException("Failed to encode source.", ioex);
    }
  }
  
  public T fromBytes(final byte[] bytes, final DatumReader<T> reader) {
    try {
      return serDe.deserialize(bytes, reader);
    } catch (IOException ioex) {
      throw new SerDeException("Failed to decode bytes.", ioex);
    }
  }
  
}
