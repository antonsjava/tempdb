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


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import sk.antons.tempdb.TreeDbBuilder;
import sk.antons.tempdb.serialization.StringSerialization;

/**
 *
 * @author antons
 */
public class AvlTreeDbTestik {
	private static Logger log = Logger.getLogger(AvlTreeDbTestik.class.getName());


    private static void testList(List<String> list, boolean times) {
        AvlTreeDb<String, String> db = TreeDbBuilder.instance(String.class, String.class)
            .tempfile("test", ".db", true)
            .keyserializer(StringSerialization.serializer())
            .keydeserializer(StringSerialization.deserializer())
            .serializer(StringSerialization.serializer())
            .deserializer(StringSerialization.deserializer())
            .avlTreeDb();
//        MapTreeDb<String, String> db = TreeDbBuilder.instance(String.class, String.class)
//            .tempfile("test", ".db", true)
//            .serializer(StringSerialization.serializer())
//            .deserializer(StringSerialization.deserializer())
//            .mapTreeDb();
        
        long time1 = System.currentTimeMillis();
        for(String string : list) {
            db.put(string, string);
        }
        long time2 = System.currentTimeMillis();

        Map<String, Integer> map = new HashMap<String, Integer>();
        for(String string : list) {
            Integer i = map.get(string);
            if(i == null) i = 0;
            i = i + 1;
            map.put(string, i);
        }

        Collections.reverse(list);
        long time3 = System.currentTimeMillis();
        for(String string : list) {
            List<String> data = db.get(string);
            if(data == null) {
                System.out.println("TEST source: " + list);
                System.out.println("TEST search: " + string);
                System.out.println("TEST data: " + data);
                System.out.println("TEST result: no data");
                continue;
            }
            if(data.isEmpty()) {
                System.out.println("TEST source: " + list);
                System.out.println("TEST search: " + string);
                System.out.println("TEST data: " + data);
                System.out.println("TEST result: no data");
                continue;
            }
            if(data.size() != map.get(string).intValue()) {
                System.out.println("TEST source: " + list);
                System.out.println("TEST search: " + string);
                System.out.println("TEST data: " + data);
                System.out.println("TEST result: data size wrong");
                continue;
            }

            boolean same = true;
            for(String string1 : data) {
                if(!string.equals(string1)) {
                    same = false;
                    break;
                }
            }
            
            if(!same) {
                System.out.println("TEST source: " + list);
                System.out.println("TEST search: " + string);
                System.out.println("TEST data: " + data);
                System.out.println("TEST result: not same as search");
                continue;
            }
        }
        long time4 = System.currentTimeMillis();
        //System.out.println(db.dump());        

        if(times) {
            System.out.println(" time dbinit: " + (time2-time1));
            System.out.println(" time dbcheck: " + (time4-time3));
        }
        
        db.delete();
    }

    public static void all(int n, List<String> list) {
        if(list.size() == n) {
            testList(list, false);
        } else {
            for(int i = 0; i < n; i++) {
                list.add("" + i);
                all(n, list);
                list.remove(list.size()-1);
            }
        }
    }
    
    public static void single(int n) {
        List<String> list = new ArrayList<String>();
        Random random = new Random(System.currentTimeMillis());
        for(int i = 0; i < n; i++) {
            list.add("" + random.nextInt(10000));
            //list.add("" + i);
        }
        testList(list, true);
    }

    public static void main(String[] argv) {
        //all(6, new ArrayList<String>());
        single(100000);
    }
    
}
