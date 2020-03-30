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
import sk.antons.tempdb.serialization.StringSerialization;

/**
 *
 * @author antons
 */
public class SequenceRawDbTest {
	private static Logger log = Logger.getLogger(SequenceRawDbTest.class.getName());

    @Test
	public void baseTest() throws Exception {
        SequenceDbWriter<String> writer = SequenceDbBuilder.instance(String.class)
            .tempfile("test", ".db", true)
            .serializer(StringSerialization.serializer())
            .sequenceDbWriter();

        writer.add("jano");
        writer.add(null);
        writer.add("fero");
        writer.close();
        
        SequenceDbReader<String> reader = SequenceDbBuilder.instance(String.class)
            .dbfile(writer.dbfile())
            .deserializer(StringSerialization.deserializer())
            .sequenceDbReader();
        
        Assert.assertEquals("jano", reader.next());
        Assert.assertEquals("", reader.next());
        Assert.assertEquals("fero", reader.next());
        Assert.assertNull(reader.next());

    }
    
    @Test
	public void nullTest() throws Exception {
        SequenceDbWriter<String> writer = SequenceDbBuilder.instance(String.class)
            .tempfile("test", ".db", true)
            .serializer(StringSerialization.serializer())
            .sequenceDbWriter();

        writer.close();
        
        SequenceDbReader<String> reader = SequenceDbBuilder.instance(String.class)
            .dbfile(writer.dbfile())
            .deserializer(StringSerialization.deserializer())
            .sequenceDbReader();
        
        Assert.assertNull(reader.next());

    }
    
}
