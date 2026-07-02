/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.stormcrawler.persistence;

import java.util.Date;
import java.util.Map;
import java.util.Optional;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.InitialisationUtil;

public abstract class Scheduler {

    /** Class to use for Scheduler. Must extend the class Scheduler. */
    public static final String schedulerClassParamName = "scheduler.class";

    /**
     * Configuration of the scheduler based on the config. Should be called by
     * Scheduler.getInstance() *
     */
    protected abstract void init(Map<String, Object> stormConf);

    /**
     * Returns an optional Date indicating when the document should be refetched next, based on its
     * status. It is empty if the URL should never be refetched.
     */
    public abstract Optional<Date> schedule(Status status, Metadata metadata);

    /** Returns a Scheduler instance based on the configuration. */
    public static Scheduler getInstance(Map<String, Object> stormConf) {
        Scheduler scheduler;

        String className = ConfUtils.getString(stormConf, schedulerClassParamName);
        try {
            scheduler = InitialisationUtil.initializeFromQualifiedName(className, Scheduler.class);
        } catch (Exception e) {
            throw new RuntimeException("Can't instantiate " + className, e);
        }

        scheduler.init(stormConf);
        return scheduler;
    }
}
