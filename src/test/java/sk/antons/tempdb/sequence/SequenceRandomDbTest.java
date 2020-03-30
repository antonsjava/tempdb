/*
 * Copyright 2018 Anton Straka
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sk.antons.tempdb.sequence;


import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Test;
import sk.antons.tempdb.SequenceDbBuilder;
import sk.antons.tempdb.sequence.SequenceDb;
import sk.antons.tempdb.serialization.StringSerialization;

/**
 *
 * @author antons
 */
public class SequenceRandomDbTest {
	private static Logger log = Logger.getLogger(SequenceRandomDbTest.class.getName());

    @Test
	public void baseTest() throws Exception {
        SequenceDb<String> db = SequenceDbBuilder.instance(String.class)
            .tempfile("test", ".db", true)
            .serializer(StringSerialization.serializer())
            .deserializer(StringSerialization.deserializer())
            .sequenceDb();

        db.add("jano");
        db.add(null);
        db.add("ferowww");
        
        
        Assert.assertEquals("jano", db.next());
        Assert.assertEquals("", db.next());
        Assert.assertEquals("ferowww", db.next());
        Assert.assertNull(db.next());

        db.close();
    }
    
    @Test
	public void mixTest() throws Exception {
        SequenceDb<String> db = SequenceDbBuilder.instance(String.class)
            .tempfile("test", ".db", true)
            .serializer(StringSerialization.serializer())
            .deserializer(StringSerialization.deserializer())
            .sequenceDb();

        db.add("jano");
        Assert.assertEquals("jano", db.next());
        Assert.assertNull(db.next());
        db.add(null);
        Assert.assertEquals("", db.next());
        Assert.assertNull(db.next());
        db.add("ferowww");
        Assert.assertEquals("ferowww", db.next());
        Assert.assertNull(db.next());

        db.close();
    }
    
    @Test
	public void nullTest() throws Exception {
        SequenceDb<String> db = SequenceDbBuilder.instance(String.class)
            .tempfile("test", ".db", true)
            .serializer(StringSerialization.serializer())
            .deserializer(StringSerialization.deserializer())
            .sequenceDb();

        Assert.assertNull(db.next());

    }
    
}
