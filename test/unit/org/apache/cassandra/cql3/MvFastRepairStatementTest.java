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

package org.apache.cassandra.cql3;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

public class MvFastRepairStatementTest extends CQLTester
{
    @Test
    public void testSetOnCreate() throws Throwable
    {
        createTable("CREATE TABLE %s (key text, val int, primary key(key)) WITH mv_fast_stream = 'auto';");
        Assert.assertEquals(Config.MVFastStream.auto, currentTableMetadata().params.mvFastStream);
    }

    @Test
    public void testDefaultValueWithAlter() throws Throwable
    {
        createTable("CREATE TABLE %s (key text, val int, primary key(key));");
        Assert.assertEquals(Config.MVFastStream.never, currentTableMetadata().params.mvFastStream);
        execute("ALTER TABLE %s WITH mv_fast_stream = 'auto';");
        Assert.assertEquals(Config.MVFastStream.auto, currentTableMetadata().params.mvFastStream);
    }

    @Test
    public void testSetOnAlter() throws Throwable
    {
        createTable("CREATE TABLE %s (key text, val int, primary key(key)) WITH mv_fast_stream = 'always';");
        Assert.assertEquals(Config.MVFastStream.always, currentTableMetadata().params.mvFastStream);
        execute("ALTER TABLE %s WITH mv_fast_stream = 'auto';");
        Assert.assertEquals(Config.MVFastStream.auto, currentTableMetadata().params.mvFastStream);
    }
}
