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

package com.cognitree.kronos.scheduler.model;

public interface Messages {
    String FAILED_TO_RESOLVE_DEPENDENCY = "failed to resolve task dependency";
    String FAILED_DEPENDEE_TASK = FAILED_TO_RESOLVE_DEPENDENCY + ", failed dependee task";
    String TIMED_OUT = "timed out executing task";
    String TASK_SUBMISSION_FAILED = "error submitting task to queue";
}
