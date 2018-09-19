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

package com.cognitree.kronos.scheduler.store;

import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.Job.Status;
import com.cognitree.kronos.scheduler.model.JobId;

import java.util.List;
import java.util.Map;

/**
 * An interface exposing API's to provide {@link Job} persistence.
 */
public interface JobStore extends Store<Job, JobId> {

    List<Job> load(String namespace) throws StoreException;

    List<Job> load(String namespace, long createdAfter, long createdBefore) throws StoreException;

    Job load(String jobId, String namespace) throws StoreException;

    List<Job> loadByWorkflowName(String workflowName, String namespace,
                                 long createdAfter, long createdBefore) throws StoreException;

    List<Job> loadByWorkflowNameAndTriggerName(String workflowName, String triggerName, String namespace,
                                               long createdAfter, long createdBefore) throws StoreException;

    Map<Status, Integer> groupByStatus(String namespace, long createdAfter, long createdBefore) throws StoreException;

    Map<Status, Integer> groupByStatusForWorkflowName(String workflowName, String namespace,
                                                      long createdAfter, long createdBefore) throws StoreException;
}