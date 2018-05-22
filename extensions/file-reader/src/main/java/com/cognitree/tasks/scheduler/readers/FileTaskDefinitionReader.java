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

package com.cognitree.tasks.scheduler.readers;

import com.cognitree.tasks.model.TaskDefinition;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * A task definition reader implementation loading tasks from specified file in the classpath
 */
public class FileTaskDefinitionReader implements TaskDefinitionReader {
    private static final Logger logger = LoggerFactory.getLogger(FileTaskDefinitionReader.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
    private static final TypeReference<List<TaskDefinition>> LIST_TYPE_REFERENCE = new TypeReference<List<TaskDefinition>>() {
    };

    private String source;

    @Override
    public void init(ObjectNode readerConfig) {
        source = readerConfig.get("source").asText();
    }

    @Override
    public List<TaskDefinition> load() throws IOException {
        logger.info("Loading task definition from source {}", source);
        return OBJECT_MAPPER.readValue(this.getClass().getClassLoader().getResourceAsStream(source), LIST_TYPE_REFERENCE);
    }
}
