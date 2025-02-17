/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.server.master.engine.system.event;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.dolphinscheduler.server.master.cluster.WorkerServerMetadata;

import java.util.Date;

import lombok.Getter;

@Getter
public class WorkerFailoverEvent extends AbstractSystemEvent {

    private final WorkerServerMetadata workerServerMetadata;
    private final Date eventTime;

    private WorkerFailoverEvent(final WorkerServerMetadata workerServerMetadata,
                                final Date eventTime,
                                final long delayTime) {
        super(delayTime);
        this.workerServerMetadata = workerServerMetadata;
        this.eventTime = eventTime;
    }

    public static WorkerFailoverEvent of(final WorkerServerMetadata workerServerMetadata,
                                         final Date eventTime,
                                         final long delayTime) {
        checkNotNull(workerServerMetadata);
        checkNotNull(eventTime);
        return new WorkerFailoverEvent(workerServerMetadata, eventTime, delayTime);
    }

    @Override
    public SystemEventType getEventType() {
        return SystemEventType.WORKER_FAILOVER;
    }

    @Override
    public String toString() {
        return "WorkerFailoverEvent{" +
                "workerServerMetadata='" + workerServerMetadata + '\'' +
                ", eventTime=" + eventTime +
                ", delayTime=" + delayTime +
                '}';
    }
}
