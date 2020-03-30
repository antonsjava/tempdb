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
package sk.antons.tempdb.tree;


import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Test;
import sk.antons.tempdb.TreeDbBuilder;
import sk.antons.tempdb.serialization.StringSerialization;

/**
 *
 * @author antons
 */
public class AvlTreeDbTest {
	private static Logger log = Logger.getLogger(AvlTreeDbTest.class.getName());

    @Test
	public void baseTest() throws Exception {
        AvlTreeDb<String, String> db = TreeDbBuilder.instance(String.class, String.class)
            .tempfile("test", ".db", true)
            .keyserializer(StringSerialization.serializer())
            .keydeserializer(StringSerialization.deserializer())
            .serializer(StringSerialization.serializer())
            .deserializer(StringSerialization.deserializer())
            .avlTreeDb();
        
        System.out.println(" ------ 1 \n" + db.dump());
        db.put("jano", "jano");
        System.out.println(" ------ 2 \n" + db.dump());
        db.put("ferowww", "ferowww");
        System.out.println(" ------ 3 \n" + db.dump());
        
        Assert.assertEquals("jano", db.get("jano").get(0));
        Assert.assertEquals("ferowww", db.get("ferowww").get(0));

        db.close();
    }
    
    
}
