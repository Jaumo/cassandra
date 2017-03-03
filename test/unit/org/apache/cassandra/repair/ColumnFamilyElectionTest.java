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
package org.apache.cassandra.repair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamType;
import org.apache.cassandra.transport.ProtocolVersion;

public class ColumnFamilyElectionTest extends CQLTester
{

    ProtocolVersion protocolVersion = ProtocolVersion.V4;
    private final List<String> views = new ArrayList<>();

    @BeforeClass
    public static void startup()
    {
        requireNetwork();
    }

    @Before
    public void begin()
    {
        views.clear();
    }

    @After
    public void end() throws Throwable
    {
        for (String viewName : views)
            executeNet(protocolVersion, "DROP MATERIALIZED VIEW " + viewName);
    }

    private void createView(String name, String query) throws Throwable
    {
        executeNet(protocolVersion, String.format(query, name));
        // If exception is thrown, the view will not be added to the list; since it shouldn't have been created, this is
        // the desired behavior
        views.add(name);
    }

    private Set<String> getSetFromIterable(Iterable<ColumnFamilyStore> iterable)
    {
        HashSet<String> set = new HashSet<String>();
        for (ColumnFamilyStore cfs : iterable)
        {
            set.add(cfs.name);
        }
        return set;
    }

    protected void assertCfs(String[] expected, Iterable<ColumnFamilyStore> result)
    {
        Set<String> set = getSetFromIterable(result);
        for (String cfName : expected)
        {
            if (!set.remove(cfName))
            {
                Assert.fail(cfName + " not contained in result set");
            }
        }

        if (set.size() > 0)
        {
            Assert.fail(set.toArray()[0] + " in result set but not expected");
        }
    }

    protected void assertCfs(String[] expected, StreamType type) throws Throwable
    {
        Iterable<ColumnFamilyStore> validColumnFamilies = StorageService.getValidColumnFamiliesForStreamType(type, keyspace());
        assertCfs(expected, validColumnFamilies);
    }

    protected void assertCfs(String[] expected) throws Throwable
    {
        assertCfs(expected, StreamType.REPAIR);
    }

    @Test
    public void testElectColumnFamiliesAuto() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (" +
                                       "k int, " +
                                       "c1 int, " +
                                       "c2 int, " +
                                       "val1 text, " +
                                       "PRIMARY KEY(k, c1)" +
                                       ") WITH mv_fast_stream = 'auto'");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        assertCfs(new String[]{
        tableName
        });

        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s " +
                          "WHERE k IS NOT NULL AND c1 IS NOT NULL " +
                          "PRIMARY KEY (c1, k)");
        // View selected because mv_fast_stream = auto resulted in fast stream
        assertCfs(new String[]{
        tableName, "mv1"
        });

        createView("mv2", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s " +
                          "WHERE val1 IS NOT NULL AND k IS NOT NULL AND c1 IS NOT NULL " +
                          "PRIMARY KEY (c1, k, val1)");

        // Views not selected because mv_fast_stream = auto resulted in write patch
        assertCfs(new String[]{
        tableName
        });

        // Boostrap requires all of them
        assertCfs(new String[]{
        tableName, "mv1", "mv2"
        }, StreamType.BOOTSTRAP);
    }

    @Test
    public void testElectColumnFamiliesFastStreamNeverAndAlways() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (" +
                                       "k int, " +
                                       "c1 int, " +
                                       "c2 int, " +
                                       "val1 text, " +
                                       "PRIMARY KEY(k, c1)" +
                                       ") WITH mv_fast_stream = 'never'");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        assertCfs(new String[]{
        tableName
        });

        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s " +
                          "WHERE k IS NOT NULL AND c1 IS NOT NULL " +
                          "PRIMARY KEY (c1, k)");
        assertCfs(new String[]{
        tableName
        });

        // Boostrap requires all of them
        assertCfs(new String[]{
        tableName, "mv1"
        }, StreamType.BOOTSTRAP);

        createView("mv2", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s " +
                          "WHERE val1 IS NOT NULL AND k IS NOT NULL AND c1 IS NOT NULL " +
                          "PRIMARY KEY (c1, k, val1)");

        // mv_fast_stream = always selects no views
        assertCfs(new String[]{
        tableName
        });

        // Boostrap requires all of them
        assertCfs(new String[]{
        tableName, "mv1", "mv2"
        }, StreamType.BOOTSTRAP);

        // mv_fast_stream = always selects all views
        alterTable("ALTER TABLE %s WITH mv_fast_stream = 'always'");
        assertCfs(new String[]{
        tableName, "mv1", "mv2"
        });

    }

}
