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

package com.cognitree.kronos.scheduler.store.jdbc;

public interface StdJDBCConstants {

    String TABLE_NAMESPACES = "NAMESPACES";
    String TABLE_WORKFLOWS = "WORKFLOWS";
    String TABLE_TASKS = "TASKS";
    String TABLE_WORKFLOW_TRIGGERS = "WORKFLOW_TRIGGERS";
    String TABLE_JOBS = "JOBS";

    String COL_NAMESPACE = "NAMESPACE";
    String COL_WORKFLOW_NAME = "WORKFLOW_NAME";
    String COL_CREATED_AT = "CREATED_AT";
    String COL_STATUS = "STATUS";
    String COL_ID = "ID";
    String COL_COMPLETED_AT = "COMPLETED_AT";
    String COL_DESCRIPTION = "DESCRIPTION";
    String COL_NAME = "NAME";
    String COL_STATUS_MESSAGE = "STATUS_MESSAGE";
    String COL_SUBMITTED_AT = "SUBMITTED_AT";
    String COL_CONTEXT = "CONTEXT";
    String COL_JOB_ID = "JOB_ID";
    String COL_TASKS = "TASKS";
    String COL_START_AT = "START_AT";
    String COL_SCHEDULE = "SCHEDULE";
    String COL_END_AT = "END_AT";
    String COL_ENABLED = "ENABLED";
    String COL_TRIGGER_NAME = "trigger_name";
    String COL_PROPERTIES = "properties";
}
