/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognitree.kronos.queue.producer;


import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * responsible to store the data of type {@code T} it receives to the underlying queue
 */
public interface Producer {

    void init(ObjectNode config);

    /**
     * sends the record to the underlying queue
     *
     * @param topic  topic name to send data
     * @param record record to send
     */
    void send(String topic, String record);

    void close();
}
