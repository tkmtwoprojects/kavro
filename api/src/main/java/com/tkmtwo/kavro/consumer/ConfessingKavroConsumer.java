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

import org.apache.avro.generic.GenericContainer;

/**
 *
 * @param <T> - a GenericContainer
 */
public class ConfessingKavroConsumer<T extends GenericContainer>
  extends KavroConsumer<T> {
  
  public ConfessingKavroConsumer(final Class<T> dc) {
    super(dc);
  }
  
  protected void doWithDatum(T t) {
    logger.info("Confession says: {}", t.toString());
  }
  
}
