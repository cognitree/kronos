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

package com.cognitree.kronos.util;

import static java.util.concurrent.TimeUnit.*;

public class DateTimeUtil {

    /**
     * resolves duration (1s, 1d, 1h etc) to milliseconds
     *
     * @param duration
     * @return
     */
    public static long resolveDuration(String duration) {
        final int interval = Integer.parseInt(duration.substring(0, duration.length() - 1));
        final char timeUnit = duration.charAt(duration.length() - 1);

        switch (timeUnit) {
            case 's':
                return SECONDS.toMillis(interval);
            case 'm':
                return MINUTES.toMillis(interval);
            case 'h':
                return HOURS.toMillis(interval);
            case 'd':
                return DAYS.toMillis(interval);
            default:
                return interval;
        }
    }

}
