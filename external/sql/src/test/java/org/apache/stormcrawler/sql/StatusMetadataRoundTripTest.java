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

package org.apache.stormcrawler.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Optional;
import org.apache.storm.task.OutputCollector;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.metrics.ScopedCounter;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.URLPartitioner;
import org.junit.jupiter.api.Test;

/**
 * The metadata column written by {@link StatusUpdaterBolt} is read back by {@link SQLSpout}.
 * Whatever goes in must come out unchanged, and a value must never introduce a key of its own.
 * Needs no database: the prepared statement is a recording stub.
 */
class StatusMetadataRoundTripTest {

    private static final String URL = "http://example.com/";

    /** Captures the value bound to the metadata column. */
    private static class MetadataCapture implements InvocationHandler {
        String metadataColumn;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            if ("setString".equals(method.getName()) && Integer.valueOf(4).equals(args[0])) {
                metadataColumn = (String) args[1];
            }
            if ("executeUpdate".equals(method.getName())) {
                return Integer.valueOf(1);
            }
            return null;
        }
    }

    private static void set(Object target, Class<?> owner, String name, Object value)
            throws Exception {
        Field f = owner.getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }

    /** Runs store() against a recording statement and returns the metadata column it wrote. */
    private static String storedMetadataColumn(Metadata metadata) throws Exception {
        StatusUpdaterBolt bolt = new StatusUpdaterBolt();
        URLPartitioner partitioner = new URLPartitioner();
        partitioner.configure(new HashMap<>());
        MetadataCapture capture = new MetadataCapture();
        PreparedStatement statement =
                (PreparedStatement)
                        Proxy.newProxyInstance(
                                StatusMetadataRoundTripTest.class.getClassLoader(),
                                new Class<?>[] {PreparedStatement.class},
                                capture);
        ScopedCounter counter = scope -> incrementBy -> {};
        set(bolt, StatusUpdaterBolt.class, "partitioner", partitioner);
        set(bolt, StatusUpdaterBolt.class, "updatePreparedStmt", statement);
        set(bolt, StatusUpdaterBolt.class, "eventCounter", counter);
        set(bolt, bolt.getClass().getSuperclass(), "useCache", Boolean.FALSE);
        set(
                bolt,
                bolt.getClass().getSuperclass(),
                "collector",
                new OutputCollector(new TestOutputCollector()));
        bolt.store(URL, Status.FETCHED, metadata, Optional.empty(), null);
        return capture.metadataColumn;
    }

    /** Reads the column back the way SQLSpout does. */
    private static Metadata readBack(String metadataColumn) {
        return MetadataColumn.decode(metadataColumn);
    }

    @Test
    void tabInAValueDoesNotBecomeAKey() throws Exception {
        Metadata metadata = new Metadata();
        // _redirTo is persisted by default and holds the raw Location header
        metadata.setValue("_redirTo", "/a\tstatus.store.as.is.with.nextfetchdate=NOT-A-DATE");
        Metadata back = readBack(storedMetadataColumn(metadata));
        assertNull(
                back.getFirstValue("status.store.as.is.with.nextfetchdate"),
                "a stored value must not introduce a key when it is read back");
        assertEquals(
                metadata.getFirstValue("_redirTo"),
                back.getFirstValue("_redirTo"),
                "the stored value must come back unchanged");
    }

    @Test
    void tabbedValueDoesNotMintDepthKeys() throws Exception {
        Metadata metadata = new Metadata();
        metadata.setValue("_redirTo", "/a\tdepth=0\tmax.depth=99999");
        Metadata back = readBack(storedMetadataColumn(metadata));
        assertNull(back.getFirstValue("depth"), "depth must not be settable from a stored value");
        assertNull(
                back.getFirstValue("max.depth"),
                "max.depth must not be settable from a stored value");
    }

    @Test
    void separatorsAndBackslashesInValuesSurviveTheRoundTrip() throws Exception {
        Metadata metadata = new Metadata();
        metadata.setValue("_redirTo", "/a?b=c\td\\e\nf\rg\\t");
        metadata.setValues("multi", new String[] {"first=1", "second\ttoo"});
        Metadata back = readBack(storedMetadataColumn(metadata));
        assertEquals("/a?b=c\td\\e\nf\rg\\t", back.getFirstValue("_redirTo"));
        assertEquals("first=1", back.getValues("multi")[0]);
        assertEquals("second\ttoo", back.getValues("multi")[1]);
    }

    @Test
    void columnWrittenBeforeTheEscapingIsStillRead() {
        Metadata back = readBack("\tdepth=1\t_redirTo=http://example.com/a");
        assertEquals("1", back.getFirstValue("depth"));
        assertEquals("http://example.com/a", back.getFirstValue("_redirTo"));
    }

    @Test
    void columnWrittenBeforeTheEscapingWithoutALeadingTabulationIsStillRead() {
        Metadata back = readBack("v1key=value\tdepth=1");
        assertEquals("value", back.getFirstValue("v1key"), "the marker is not a key prefix");
        assertEquals("1", back.getFirstValue("depth"));
    }

    @Test
    void columnWrittenBeforeTheEscapingWithAnEmptyKeyIsRewrittenWithoutIt() throws Exception {
        Metadata back = readBack("\t=x\tdepth=1");
        assertEquals("1", back.getFirstValue("depth"));
        assertNull(back.getFirstValue(""), "an empty key cannot be read back from a metadata");
        assertEquals(
                "v1\tdepth=1",
                storedMetadataColumn(back),
                "a key which cannot be read back is not written out again");
    }

    @Test
    void emptyColumnGivesEmptyMetadata() {
        assertEquals(0, readBack(null).size());
        assertEquals(0, readBack("").size());
        assertEquals(0, readBack(MetadataColumn.encode(new Metadata())).size());
    }
}
