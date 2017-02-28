/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.streaming;

public enum StreamOperation
{

    OTHER("Other"), // Fallback to avoid null types when deserializing from string
    RESTORE_REPLICA_COUNT("Restore replica count"), // Handles removeNode
    DECOMMISSION("Unbootstrap"),
    RELOCATION("Relocation"),
    BOOTSTRAP("Bootstrap"),
    REBUILD("Rebuild"),
    BULK_LOAD("Bulk Load"),
    REPAIR("Repair")
    ;

    private final String type;

    StreamOperation(final String type) {
        this.type = type;
    }

    public static StreamOperation fromString(String text) {
        for (StreamOperation b : StreamOperation.values()) {
            if (b.type.equalsIgnoreCase(text)) {
                return b;
            }
        }

        return OTHER;
    }

    public String getDescription() {
        return type;
    }
}
