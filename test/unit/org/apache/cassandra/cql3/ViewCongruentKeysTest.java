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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import junit.framework.Assert;
import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertTrue;

public class ViewCongruentKeysTest extends CQLTester
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

    @Test
    public void testCongruency() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                         "k int, " +
                         "c1 int, " +
                         "c2 int, " +
                         "val1 text, " +
                         "PRIMARY KEY(k, c1)" +
                         ")");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Assert.assertTrue(getCurrentColumnFamilyStore().viewManager.primaryKeysOfViewsAreCongruent());

        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s " +
                          "WHERE k IS NOT NULL AND c1 IS NOT NULL " +
                          "PRIMARY KEY (c1, k)");
        Assert.assertTrue(getCurrentColumnFamilyStore().viewManager.primaryKeysOfViewsAreCongruent());
        Assert.assertTrue(getCurrentColumnFamilyStore().viewManager.getByName("mv1").getDefinition().primaryKeyIsCongruentToBaseTable());

        createView("mv2", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s " +
                          "WHERE val1 IS NOT NULL AND k IS NOT NULL AND c1 IS NOT NULL " +
                          "PRIMARY KEY (c1, k, val1)");
        Assert.assertFalse(getCurrentColumnFamilyStore().viewManager.getByName("mv2").getDefinition().primaryKeyIsCongruentToBaseTable());
        Assert.assertFalse(getCurrentColumnFamilyStore().viewManager.primaryKeysOfViewsAreCongruent());

        executeNet(protocolVersion, "DROP MATERIALIZED VIEW mv2");
        views.remove("mv2");
        Assert.assertTrue(getCurrentColumnFamilyStore().viewManager.primaryKeysOfViewsAreCongruent());

        executeNet(protocolVersion, "DROP MATERIALIZED VIEW mv1");
        views.remove("mv1");
        Assert.assertTrue(getCurrentColumnFamilyStore().viewManager.primaryKeysOfViewsAreCongruent());
    }

}
