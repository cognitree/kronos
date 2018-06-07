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

package com.cognitree.kronos.scheduler.readers;

import com.cognitree.kronos.model.TaskDefinition;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

/**
 * An interface implemented to provide custom source of {@link TaskDefinition}.
 */
public interface TaskDefinitionReader {

    /**
     * for each reader during initialization phase a call is made to initialize reader using
     * {@link TaskDefinitionReaderConfig#config}. Any property required by the reader to instantiate itself
     * should be part of {@link TaskDefinitionReaderConfig#config}.
     *
     * @param readerConfig configuration used to initialize the reader.
     */
    void init(ObjectNode readerConfig);

    List<TaskDefinition> load() throws Exception;
}
