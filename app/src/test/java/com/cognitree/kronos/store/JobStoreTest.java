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

package com.cognitree.kronos.store;

import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.store.JobStore;
import com.cognitree.kronos.scheduler.store.NamespaceStore;
import com.cognitree.kronos.scheduler.store.StoreException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.cognitree.kronos.scheduler.model.Job.Status.CREATED;
import static com.cognitree.kronos.scheduler.model.Job.Status.FAILED;
import static com.cognitree.kronos.scheduler.model.Job.Status.RUNNING;
import static com.cognitree.kronos.scheduler.model.Job.Status.SUCCESSFUL;

public class JobStoreTest extends StoreTest {

    @Before
    public void setup() throws Exception {
        // initialize store with some jobs
        for (int i = 0; i < 5; i++) {
            createJob(UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
    }

    @Test
    public void testStoreAndLoadJobs() throws StoreException {
        ArrayList<Job> existingJobs = loadExistingJobs();
        JobStore jobStore = storeService.getJobStore();

        String namespace = UUID.randomUUID().toString();
        String workflow = UUID.randomUUID().toString();
        String trigger = UUID.randomUUID().toString();
        Job jobOne = createJob(namespace, workflow, trigger, UUID.randomUUID().toString());
        Job jobTwo = createJob(namespace, workflow, trigger, UUID.randomUUID().toString());
        List<Job> jobsByNs = jobStore.load(namespace);
        assertEquals(2, jobsByNs.size());
        Assert.assertTrue(jobsByNs.contains(jobOne));
        Assert.assertTrue(jobsByNs.contains(jobTwo));
        assertEquals(jobOne, jobStore.load(jobOne));
        assertEquals(jobTwo, jobStore.load(jobTwo));

        List<Job> jobsByStatusCreated = jobStore.loadByStatus(namespace, Collections.singletonList(CREATED),
                0, System.currentTimeMillis());
        assertEquals(2, jobsByStatusCreated.size());
        Assert.assertTrue(jobsByStatusCreated.contains(jobOne));
        Assert.assertTrue(jobsByStatusCreated.contains(jobTwo));

        assertEquals(Collections.emptyList(),
                jobStore.loadByStatus(namespace, Collections.singletonList(RUNNING), 0, System.currentTimeMillis()));
        assertEquals(Collections.emptyList(),
                jobStore.loadByStatus(namespace, Collections.singletonList(SUCCESSFUL), 0, System.currentTimeMillis()));
        assertEquals(Collections.emptyList(),
                jobStore.loadByStatus(namespace, Collections.singletonList(FAILED), 0, System.currentTimeMillis()));

        List<Job> jobsByWorkflowName = jobStore.loadByWorkflowName(namespace, workflow, 0, System.currentTimeMillis());
        assertEquals(2, jobsByWorkflowName.size());
        Assert.assertTrue(jobsByWorkflowName.contains(jobOne));
        Assert.assertTrue(jobsByWorkflowName.contains(jobTwo));

        List<Job> jobsByStatusCreatedAndWorkflow = jobStore.loadByWorkflowNameAndStatus(namespace, workflow,
                Collections.singletonList(CREATED), 0, System.currentTimeMillis());
        assertEquals(2, jobsByStatusCreatedAndWorkflow.size());
        Assert.assertTrue(jobsByStatusCreatedAndWorkflow.contains(jobOne));
        Assert.assertTrue(jobsByStatusCreatedAndWorkflow.contains(jobTwo));

        assertEquals(Collections.emptyList(),
                jobStore.loadByWorkflowNameAndStatus(namespace, workflow,
                        Collections.singletonList(RUNNING), 0, System.currentTimeMillis()));
        assertEquals(Collections.emptyList(),
                jobStore.loadByWorkflowNameAndStatus(namespace, workflow,
                        Collections.singletonList(SUCCESSFUL), 0, System.currentTimeMillis()));
        assertEquals(Collections.emptyList(),
                jobStore.loadByWorkflowNameAndStatus(namespace, workflow,
                        Collections.singletonList(FAILED), 0, System.currentTimeMillis()));

        List<Job> jobsByWorkflowAndTrigger = jobStore.loadByWorkflowNameAndTriggerName(namespace, workflow,
                trigger, 0, System.currentTimeMillis());

        assertEquals(2, jobsByWorkflowAndTrigger.size());
        Assert.assertTrue(jobsByWorkflowAndTrigger.contains(jobOne));
        Assert.assertTrue(jobsByWorkflowAndTrigger.contains(jobTwo));

        List<Job> jobsByStatusCreatedAndWorkflowAndTrigger =
                jobStore.loadByWorkflowNameAndTriggerNameAndStatus(namespace, workflow, trigger,
                        Collections.singletonList(CREATED), 0, System.currentTimeMillis());
        assertEquals(2, jobsByStatusCreatedAndWorkflowAndTrigger.size());
        Assert.assertTrue(jobsByStatusCreatedAndWorkflowAndTrigger.contains(jobOne));
        Assert.assertTrue(jobsByStatusCreatedAndWorkflowAndTrigger.contains(jobTwo));

        assertEquals(Collections.emptyList(),
                jobStore.loadByWorkflowNameAndTriggerNameAndStatus(namespace, workflow, trigger,
                        Collections.singletonList(RUNNING), 0, System.currentTimeMillis()));
        assertEquals(Collections.emptyList(),
                jobStore.loadByWorkflowNameAndTriggerNameAndStatus(namespace, workflow, trigger,
                        Collections.singletonList(SUCCESSFUL), 0, System.currentTimeMillis()));
        assertEquals(Collections.emptyList(),
                jobStore.loadByWorkflowNameAndTriggerNameAndStatus(namespace, workflow, trigger,
                        Collections.singletonList(FAILED), 0, System.currentTimeMillis()));

        Map<Job.Status, Integer> countJobsByStatus = jobStore.countByStatus(namespace, 0, System.currentTimeMillis());
        assertEquals(2, countJobsByStatus.get(CREATED) == null ? 0 : countJobsByStatus.get(CREATED).intValue());
        assertEquals(0, countJobsByStatus.get(SUCCESSFUL) == null ? 0 : countJobsByStatus.get(SUCCESSFUL).intValue());
        assertEquals(0, countJobsByStatus.get(FAILED) == null ? 0 : countJobsByStatus.get(FAILED).intValue());
        assertEquals(0, countJobsByStatus.get(RUNNING) == null ? 0 : countJobsByStatus.get(RUNNING).intValue());

        ArrayList<Job> jobsAfterCreate = loadExistingJobs();
        jobsAfterCreate.remove(jobOne);
        jobsAfterCreate.remove(jobTwo);

        Assert.assertTrue("Jobs loaded from store do not match with expected",
                jobsAfterCreate.size() == existingJobs.size() &&
                        jobsAfterCreate.containsAll(existingJobs)
                        && existingJobs.containsAll(jobsAfterCreate));
    }

    @Test
    public void testUpdateJob() throws StoreException {
        ArrayList<Job> existingJobs = loadExistingJobs();
        JobStore jobStore = storeService.getJobStore();

        Job job = createJob(UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                UUID.randomUUID().toString(), UUID.randomUUID().toString());
        job.setStatus(SUCCESSFUL);
        jobStore.update(job);
        assertEquals(job, jobStore.load(job));

        job.setCompletedAt(System.currentTimeMillis());
        jobStore.update(job);
        assertEquals(job, jobStore.load(job));

        job.setCreatedAt(System.currentTimeMillis());
        jobStore.update(job);
        assertEquals(job, jobStore.load(job));

        ArrayList<Job> jobsAfterCreate = loadExistingJobs();
        jobsAfterCreate.remove(job);
        Assert.assertTrue("Jobs loaded from store do not match with expected",
                jobsAfterCreate.size() == existingJobs.size() &&
                        jobsAfterCreate.containsAll(existingJobs)
                        && existingJobs.containsAll(jobsAfterCreate));
    }

    @Test
    public void testCountByJobStatus() throws StoreException {
        ArrayList<Job> existingJobs = loadExistingJobs();
        JobStore jobStore = storeService.getJobStore();

        String namespace = UUID.randomUUID().toString();
        String workflow = UUID.randomUUID().toString();
        String trigger = UUID.randomUUID().toString();
        Job jobOne = createJob(namespace, workflow, trigger, UUID.randomUUID().toString());
        Job jobTwo = createJob(namespace, workflow, trigger, UUID.randomUUID().toString());
        Job jobThree = createJob(namespace, workflow, trigger, UUID.randomUUID().toString());
        Job jobFour = createJob(namespace, workflow, trigger, UUID.randomUUID().toString());
        Job jobFive = createJob(namespace, workflow, trigger, UUID.randomUUID().toString());
        List<Job> jobsByNs = jobStore.load(namespace);
        assertEquals(5, jobsByNs.size());

        Map<Job.Status, Integer> countJobsByStatus = jobStore.countByStatus(namespace, 0, System.currentTimeMillis());
        assertEquals(5, countJobsByStatus.get(CREATED) == null ? 0 : countJobsByStatus.get(CREATED).intValue());
        assertEquals(0, countJobsByStatus.get(SUCCESSFUL) == null ? 0 : countJobsByStatus.get(SUCCESSFUL).intValue());
        assertEquals(0, countJobsByStatus.get(FAILED) == null ? 0 : countJobsByStatus.get(FAILED).intValue());
        assertEquals(0, countJobsByStatus.get(RUNNING) == null ? 0 : countJobsByStatus.get(RUNNING).intValue());

        jobOne.setStatus(CREATED);
        jobStore.update(jobOne);
        jobTwo.setStatus(RUNNING);
        jobStore.update(jobTwo);
        jobThree.setStatus(SUCCESSFUL);
        jobStore.update(jobThree);
        jobFour.setStatus(FAILED);
        jobStore.update(jobFour);
        jobFive.setStatus(SUCCESSFUL);
        jobStore.update(jobFive);

        countJobsByStatus = jobStore.countByStatus(namespace, 0, System.currentTimeMillis());
        assertEquals(1, countJobsByStatus.get(CREATED) == null ? 0 : countJobsByStatus.get(CREATED).intValue());
        assertEquals(1, countJobsByStatus.get(RUNNING) == null ? 0 : countJobsByStatus.get(RUNNING).intValue());
        assertEquals(2, countJobsByStatus.get(SUCCESSFUL) == null ? 0 : countJobsByStatus.get(SUCCESSFUL).intValue());
        assertEquals(1, countJobsByStatus.get(FAILED) == null ? 0 : countJobsByStatus.get(FAILED).intValue());

        ArrayList<Job> jobsAfterCreate = loadExistingJobs();
        jobsAfterCreate.remove(jobOne);
        jobsAfterCreate.remove(jobTwo);
        jobsAfterCreate.remove(jobThree);
        jobsAfterCreate.remove(jobFour);
        jobsAfterCreate.remove(jobFive);

        Assert.assertTrue("Jobs loaded from store do not match with expected",
                jobsAfterCreate.size() == existingJobs.size() &&
                        jobsAfterCreate.containsAll(existingJobs)
                        && existingJobs.containsAll(jobsAfterCreate));
    }

    @Test
    public void testLoadByWorkflowNameCreatedBeforeAndAfter() throws StoreException {
        ArrayList<Job> existingJobs = loadExistingJobs();
        JobStore jobStore = storeService.getJobStore();

        String namespace = UUID.randomUUID().toString();
        String workflow = UUID.randomUUID().toString();
        String trigger = UUID.randomUUID().toString();
        long createdAt = System.currentTimeMillis();
        Job jobOne = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt);
        Job jobTwo = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt + 100);
        Job jobThree = createJob(namespace, UUID.randomUUID().toString(), trigger, UUID.randomUUID().toString(), createdAt);
        Job jobFour = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt - 100);
        Job jobFive = createJob(namespace, UUID.randomUUID().toString(), trigger, UUID.randomUUID().toString(), createdAt);

        List<Job> jobs = jobStore.loadByWorkflowName(namespace, workflow, createdAt, createdAt + 100);
        assertEquals(0, jobs.size());

        jobs = jobStore.loadByWorkflowName(namespace, workflow, createdAt - 100, createdAt);
        assertEquals(0, jobs.size());

        jobs = jobStore.loadByWorkflowName(namespace, workflow, createdAt - 1, createdAt + 1);
        assertEquals(1, jobs.size());

        Job jobSix = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt);
        jobs = jobStore.loadByWorkflowName(namespace, workflow, createdAt - 1, createdAt + 1);
        assertEquals(2, jobs.size());

        jobs = jobStore.loadByWorkflowName(namespace, jobThree.getWorkflow(), createdAt - 1, createdAt + 1);
        assertEquals(1, jobs.size());

        jobs = jobStore.loadByWorkflowName(namespace, jobFive.getWorkflow(), createdAt - 1, createdAt + 1);
        assertEquals(1, jobs.size());

        ArrayList<Job> jobsAfterCreate = loadExistingJobs();
        jobsAfterCreate.remove(jobOne);
        jobsAfterCreate.remove(jobTwo);
        jobsAfterCreate.remove(jobThree);
        jobsAfterCreate.remove(jobFour);
        jobsAfterCreate.remove(jobFive);
        jobsAfterCreate.remove(jobSix);

        Assert.assertTrue("Jobs loaded from store do not match with expected",
                jobsAfterCreate.size() == existingJobs.size() &&
                        jobsAfterCreate.containsAll(existingJobs)
                        && existingJobs.containsAll(jobsAfterCreate));
    }

    @Test
    public void testLoadByWorkflowNameAndTriggerNameCreatedBeforeAndAfter() throws StoreException {
        ArrayList<Job> existingJobs = loadExistingJobs();
        JobStore jobStore = storeService.getJobStore();

        String namespace = UUID.randomUUID().toString();
        String workflow = UUID.randomUUID().toString();
        String trigger = UUID.randomUUID().toString();
        long createdAt = System.currentTimeMillis();
        Job jobOne = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt);
        Job jobTwo = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt + 100);
        Job jobThree = createJob(namespace, workflow, UUID.randomUUID().toString(), UUID.randomUUID().toString(), createdAt);
        Job jobFour = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt - 100);
        Job jobFive = createJob(namespace, workflow, UUID.randomUUID().toString(), UUID.randomUUID().toString(), createdAt);

        List<Job> jobs = jobStore.loadByWorkflowNameAndTriggerName(namespace, workflow, trigger,
                createdAt, createdAt + 100);
        assertEquals(0, jobs.size());

        jobs = jobStore.loadByWorkflowNameAndTriggerName(namespace, workflow, trigger, createdAt - 100, createdAt);
        assertEquals(0, jobs.size());

        jobs = jobStore.loadByWorkflowNameAndTriggerName(namespace, workflow, trigger, createdAt - 1, createdAt + 1);
        assertEquals(1, jobs.size());

        Job jobSix = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt);
        jobs = jobStore.loadByWorkflowNameAndTriggerName(namespace, workflow, trigger, createdAt - 1, createdAt + 1);
        assertEquals(2, jobs.size());

        jobs = jobStore.loadByWorkflowNameAndTriggerName(namespace, workflow, jobThree.getTrigger(), createdAt - 1,
                createdAt + 1);
        assertEquals(1, jobs.size());

        jobs = jobStore.loadByWorkflowNameAndTriggerName(namespace, workflow, jobFive.getTrigger(), createdAt - 1,
                createdAt + 1);
        assertEquals(1, jobs.size());

        ArrayList<Job> jobsAfterCreate = loadExistingJobs();
        jobsAfterCreate.remove(jobOne);
        jobsAfterCreate.remove(jobTwo);
        jobsAfterCreate.remove(jobThree);
        jobsAfterCreate.remove(jobFour);
        jobsAfterCreate.remove(jobFive);
        jobsAfterCreate.remove(jobSix);

        Assert.assertTrue("Jobs loaded from store do not match with expected",
                jobsAfterCreate.size() == existingJobs.size() &&
                        jobsAfterCreate.containsAll(existingJobs)
                        && existingJobs.containsAll(jobsAfterCreate));
    }

    @Test
    public void testLoadByStatusCreatedBeforeAndAfter() throws StoreException {
        ArrayList<Job> existingJobs = loadExistingJobs();
        JobStore jobStore = storeService.getJobStore();

        String namespace = UUID.randomUUID().toString();
        String workflow = UUID.randomUUID().toString();
        String trigger = UUID.randomUUID().toString();
        long createdAt = System.currentTimeMillis();
        Job jobOne = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt);
        Job jobTwo = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt + 100);
        Job jobThree = createJob(namespace, UUID.randomUUID().toString(), trigger, UUID.randomUUID().toString(), createdAt);
        Job jobFour = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt - 100);
        Job jobFive = createJob(namespace, UUID.randomUUID().toString(), trigger, UUID.randomUUID().toString(), createdAt);

        List<Job> jobs = jobStore.loadByStatus(namespace, Collections.singletonList(CREATED),
                createdAt - 100, createdAt);
        assertEquals(0, jobs.size());
        jobs = jobStore.loadByStatus(namespace, Collections.singletonList(CREATED), createdAt, createdAt + 100);
        assertEquals(0, jobs.size());

        jobs = jobStore.loadByStatus(namespace, Collections.singletonList(CREATED), createdAt - 1, createdAt + 1);
        assertEquals(3, jobs.size());

        Job jobSix = createJob(namespace, UUID.randomUUID().toString(), trigger, UUID.randomUUID().toString(), createdAt);
        jobs = jobStore.loadByStatus(namespace, Collections.singletonList(CREATED), createdAt - 1, createdAt + 1);
        assertEquals(4, jobs.size());

        ArrayList<Job> jobsAfterCreate = loadExistingJobs();
        jobsAfterCreate.remove(jobOne);
        jobsAfterCreate.remove(jobTwo);
        jobsAfterCreate.remove(jobThree);
        jobsAfterCreate.remove(jobFour);
        jobsAfterCreate.remove(jobFive);
        jobsAfterCreate.remove(jobSix);

        Assert.assertTrue("Jobs loaded from store do not match with expected",
                jobsAfterCreate.size() == existingJobs.size() &&
                        jobsAfterCreate.containsAll(existingJobs)
                        && existingJobs.containsAll(jobsAfterCreate));
    }

    @Test
    public void testLoadByWorkflowNameAndStatusCreatedBeforeAndAfter() throws StoreException {
        ArrayList<Job> existingJobs = loadExistingJobs();
        JobStore jobStore = storeService.getJobStore();

        String namespace = UUID.randomUUID().toString();
        String workflow = UUID.randomUUID().toString();
        String trigger = UUID.randomUUID().toString();
        long createdAt = System.currentTimeMillis();
        Job jobOne = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt);
        Job jobTwo = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt + 100);
        Job jobThree = createJob(namespace, UUID.randomUUID().toString(), trigger, UUID.randomUUID().toString(), createdAt);
        Job jobFour = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt - 100);
        Job jobFive = createJob(namespace, UUID.randomUUID().toString(), trigger, UUID.randomUUID().toString(), createdAt);

        List<Job> jobs = jobStore.loadByWorkflowNameAndStatus(namespace, workflow, Collections.singletonList(CREATED),
                createdAt - 100, createdAt);
        assertEquals(0, jobs.size());
        jobs = jobStore.loadByWorkflowNameAndStatus(namespace, workflow, Collections.singletonList(CREATED),
                createdAt, createdAt + 100);
        assertEquals(0, jobs.size());

        jobs = jobStore.loadByWorkflowNameAndStatus(namespace, workflow, Collections.singletonList(CREATED),
                createdAt - 1, createdAt + 1);
        assertEquals(1, jobs.size());

        Job jobSix = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt);
        jobs = jobStore.loadByWorkflowNameAndStatus(namespace, workflow, Collections.singletonList(CREATED),
                createdAt - 1, createdAt + 1);
        assertEquals(2, jobs.size());

        jobs = jobStore.loadByWorkflowNameAndStatus(namespace, jobThree.getWorkflow(), Collections.singletonList(CREATED),
                createdAt - 1, createdAt + 1);
        assertEquals(1, jobs.size());

        jobs = jobStore.loadByWorkflowNameAndStatus(namespace, jobFive.getWorkflow(), Collections.singletonList(CREATED),
                createdAt - 1, createdAt + 1);
        assertEquals(1, jobs.size());

        ArrayList<Job> jobsAfterCreate = loadExistingJobs();
        jobsAfterCreate.remove(jobOne);
        jobsAfterCreate.remove(jobTwo);
        jobsAfterCreate.remove(jobThree);
        jobsAfterCreate.remove(jobFour);
        jobsAfterCreate.remove(jobFive);
        jobsAfterCreate.remove(jobSix);

        Assert.assertTrue("Jobs loaded from store do not match with expected",
                jobsAfterCreate.size() == existingJobs.size() &&
                        jobsAfterCreate.containsAll(existingJobs)
                        && existingJobs.containsAll(jobsAfterCreate));
    }

    @Test
    public void testLoadByWorkflowNameAndTriggerNameAndStatusCreatedBeforeAndAfter() throws StoreException {
        ArrayList<Job> existingJobs = loadExistingJobs();
        JobStore jobStore = storeService.getJobStore();

        String namespace = UUID.randomUUID().toString();
        String workflow = UUID.randomUUID().toString();
        String trigger = UUID.randomUUID().toString();
        long createdAt = System.currentTimeMillis();
        Job jobOne = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt);
        Job jobTwo = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt + 100);
        Job jobThree = createJob(namespace, workflow, UUID.randomUUID().toString(), UUID.randomUUID().toString(), createdAt);
        Job jobFour = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt - 100);
        Job jobFive = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt);
        Job jobSix = createJob(namespace, workflow, UUID.randomUUID().toString(), UUID.randomUUID().toString(), createdAt);

        List<Job> jobs = jobStore.loadByWorkflowNameAndTriggerNameAndStatus(namespace, workflow, trigger,
                Collections.singletonList(CREATED), createdAt - 100, createdAt);
        assertEquals(0, jobs.size());
        jobs = jobStore.loadByWorkflowNameAndTriggerNameAndStatus(namespace, workflow, trigger,
                Collections.singletonList(CREATED), createdAt, createdAt + 100);
        assertEquals(0, jobs.size());

        jobs = jobStore.loadByWorkflowNameAndTriggerNameAndStatus(namespace, workflow, trigger,
                Collections.singletonList(CREATED), createdAt - 1, createdAt + 1);
        assertEquals(2, jobs.size());

        Job jobSeven = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt);
        jobs = jobStore.loadByWorkflowNameAndTriggerNameAndStatus(namespace, workflow, trigger,
                Collections.singletonList(CREATED), createdAt - 1, createdAt + 1);
        assertEquals(3, jobs.size());

        jobs = jobStore.loadByWorkflowNameAndTriggerNameAndStatus(namespace, workflow, jobThree.getTrigger(),
                Collections.singletonList(CREATED), createdAt - 1, createdAt + 1);
        assertEquals(1, jobs.size());

        jobs = jobStore.loadByWorkflowNameAndTriggerNameAndStatus(namespace, workflow, jobSix.getTrigger(),
                Collections.singletonList(CREATED), createdAt - 1, createdAt + 1);
        assertEquals(1, jobs.size());

        ArrayList<Job> jobsAfterCreate = loadExistingJobs();
        jobsAfterCreate.remove(jobOne);
        jobsAfterCreate.remove(jobTwo);
        jobsAfterCreate.remove(jobThree);
        jobsAfterCreate.remove(jobFour);
        jobsAfterCreate.remove(jobFive);
        jobsAfterCreate.remove(jobSix);
        jobsAfterCreate.remove(jobSeven);

        Assert.assertTrue("Jobs loaded from store do not match with expected",
                jobsAfterCreate.size() == existingJobs.size() &&
                        jobsAfterCreate.containsAll(existingJobs)
                        && existingJobs.containsAll(jobsAfterCreate));
    }

    @Test
    public void testCountByJobStatusCreatedBeforeAndAfter() throws StoreException {
        ArrayList<Job> existingJobs = loadExistingJobs();
        JobStore jobStore = storeService.getJobStore();

        String namespace = UUID.randomUUID().toString();
        String workflow = UUID.randomUUID().toString();
        String trigger = UUID.randomUUID().toString();
        long createdAt = System.currentTimeMillis();
        Job jobOne = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt);
        Job jobTwo = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt + 100);
        Job jobThree = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt);
        Job jobFour = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt - 100);
        Job jobFive = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt);

        Map<Job.Status, Integer> countJobsByStatus = jobStore.countByStatus(namespace, createdAt, createdAt + 100);
        assertEquals(0, countJobsByStatus.get(CREATED) == null ? 0 : countJobsByStatus.get(CREATED).intValue());

        countJobsByStatus = jobStore.countByStatus(namespace, createdAt - 100, createdAt);
        assertEquals(0, countJobsByStatus.get(CREATED) == null ? 0 : countJobsByStatus.get(CREATED).intValue());

        countJobsByStatus = jobStore.countByStatus(namespace, createdAt - 1, createdAt + 1);
        assertEquals(3, countJobsByStatus.get(CREATED) == null ? 0 : countJobsByStatus.get(CREATED).intValue());

        Job jobSix = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt);
        countJobsByStatus = jobStore.countByStatus(namespace, createdAt - 1, createdAt + 1);
        assertEquals(4, countJobsByStatus.get(CREATED) == null ? 0 : countJobsByStatus.get(CREATED).intValue());

        ArrayList<Job> jobsAfterCreate = loadExistingJobs();
        jobsAfterCreate.remove(jobOne);
        jobsAfterCreate.remove(jobTwo);
        jobsAfterCreate.remove(jobThree);
        jobsAfterCreate.remove(jobFour);
        jobsAfterCreate.remove(jobFive);
        jobsAfterCreate.remove(jobSix);

        Assert.assertTrue("Jobs loaded from store do not match with expected",
                jobsAfterCreate.size() == existingJobs.size() &&
                        jobsAfterCreate.containsAll(existingJobs)
                        && existingJobs.containsAll(jobsAfterCreate));
    }

    @Test
    public void testCountByStatusAndWorkflowNameCreatedBeforeAndAfter() throws StoreException {
        ArrayList<Job> existingJobs = loadExistingJobs();
        JobStore jobStore = storeService.getJobStore();

        String namespace = UUID.randomUUID().toString();
        String workflow = UUID.randomUUID().toString();
        String trigger = UUID.randomUUID().toString();
        long createdAt = System.currentTimeMillis();
        Job jobOne = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt);
        Job jobTwo = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt + 100);
        Job jobThree = createJob(namespace, UUID.randomUUID().toString(), trigger, UUID.randomUUID().toString(), createdAt);
        Job jobFour = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt - 100);
        Job jobFive = createJob(namespace, UUID.randomUUID().toString(), trigger, UUID.randomUUID().toString(), createdAt);

        Map<Job.Status, Integer> countJobsByStatus = jobStore.countByStatusForWorkflowName(namespace, workflow,
                createdAt - 100, createdAt);
        assertEquals(0, countJobsByStatus.get(CREATED) == null ? 0 : countJobsByStatus.get(CREATED).intValue());

        countJobsByStatus = jobStore.countByStatusForWorkflowName(namespace, workflow,
                createdAt, createdAt + 100);
        assertEquals(0, countJobsByStatus.get(CREATED) == null ? 0 : countJobsByStatus.get(CREATED).intValue());

        countJobsByStatus = jobStore.countByStatusForWorkflowName(namespace, workflow, createdAt - 1, createdAt + 1);
        assertEquals(1, countJobsByStatus.get(CREATED) == null ? 0 : countJobsByStatus.get(CREATED).intValue());

        Job jobSix = createJob(namespace, workflow, trigger, UUID.randomUUID().toString(), createdAt);
        countJobsByStatus = jobStore.countByStatusForWorkflowName(namespace, workflow, createdAt - 1, createdAt + 1);
        assertEquals(2, countJobsByStatus.get(CREATED) == null ? 0 : countJobsByStatus.get(CREATED).intValue());

        countJobsByStatus = jobStore.countByStatusForWorkflowName(namespace, jobThree.getWorkflow(), createdAt - 1,
                createdAt + 1);
        assertEquals(1, countJobsByStatus.get(CREATED) == null ? 0 : countJobsByStatus.get(CREATED).intValue());

        countJobsByStatus = jobStore.countByStatusForWorkflowName(namespace, jobFive.getWorkflow(), createdAt - 1,
                createdAt + 1);
        assertEquals(1, countJobsByStatus.get(CREATED) == null ? 0 : countJobsByStatus.get(CREATED).intValue());

        ArrayList<Job> jobsAfterCreate = loadExistingJobs();
        jobsAfterCreate.remove(jobOne);
        jobsAfterCreate.remove(jobTwo);
        jobsAfterCreate.remove(jobThree);
        jobsAfterCreate.remove(jobFour);
        jobsAfterCreate.remove(jobFive);
        jobsAfterCreate.remove(jobSix);

        Assert.assertTrue("Jobs loaded from store do not match with expected",
                jobsAfterCreate.size() == existingJobs.size() &&
                        jobsAfterCreate.containsAll(existingJobs)
                        && existingJobs.containsAll(jobsAfterCreate));
    }

    @Test
    public void testDeleteJob() throws StoreException {
        ArrayList<Job> existingJobs = loadExistingJobs();
        JobStore jobStore = storeService.getJobStore();

        Job jobOne = createJob(UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                UUID.randomUUID().toString(), UUID.randomUUID().toString());

        Job jobTwo = createJob(UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                UUID.randomUUID().toString(), UUID.randomUUID().toString());

        jobStore.delete(jobOne);
        Assert.assertNull(jobStore.load(jobOne));
        jobStore.deleteByWorkflowName(jobTwo.getNamespace(), jobTwo.getWorkflow());
        Assert.assertNull(jobStore.load(jobTwo));
        ArrayList<Job> jobsAfterCreate = loadExistingJobs();
        Assert.assertTrue("Jobs loaded from store do not match with expected",
                jobsAfterCreate.size() == existingJobs.size() &&
                        jobsAfterCreate.containsAll(existingJobs)
                        && existingJobs.containsAll(jobsAfterCreate));
    }

    private ArrayList<Job> loadExistingJobs() throws StoreException {
        JobStore jobStore = storeService.getJobStore();
        ArrayList<Job> existingJobs = new ArrayList<>();
        NamespaceStore namespaceStore = storeService.getNamespaceStore();
        List<Namespace> namespaces = namespaceStore.load();
        namespaces.forEach(namespace -> {
            try {
                List<Job> jobs = jobStore.load(namespace.getName());
                if (jobs != null && !jobs.isEmpty()) {
                    existingJobs.addAll(jobs);
                }
            } catch (StoreException e) {
                Assert.fail(e.getMessage());
            }
        });
        return existingJobs;
    }
}
