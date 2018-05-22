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

package com.cognitree.kronos.queue.consumer;

import com.cognitree.kronos.queue.Subscriber;

/**
 * responsible to provide available data of type {@code T} from the underlying queue to the subscribed subscriber
 */
public interface Consumer<T> {

    /**
     * subscriber to call whenever new data is available in the queue
     *
     * @param subscriber
     */
    void subscribe(Subscriber<T> subscriber);

    void close();
}
