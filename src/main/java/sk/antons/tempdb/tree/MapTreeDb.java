/*
 * Copyright 2020 Anton Straka
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

import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import sk.antons.tempdb.TempDbException;
import sk.antons.tempdb.base.AbstractDb;
import sk.antons.tempdb.base.DbByteArrayInputStream;
import sk.antons.tempdb.base.DbByteArrayOutputStream;
import sk.antons.tempdb.base.DbFile;
import sk.antons.tempdb.serialization.BytesDeserializer;
import sk.antons.tempdb.serialization.BytesSerializer;

/**
 * Map like database where kys are stored in memory and values in file 
 * @author antons
 */
public class MapTreeDb<K, V> extends AbstractDb {
    Map<K, List<Long>> keymap = new TreeMap<K, List<Long>>();
    protected BytesSerializer<V> serializer;
    protected BytesDeserializer<V> deserializer;
    protected RandomAccessFile raf;
    protected long index = 0;
    protected long size = 0;
    private DbByteArrayOutputStream os ;
    private DataOutputStream dos;
    private DbByteArrayInputStream is ;
    private DataInputStream dis;
    
    public MapTreeDb(DbFile dbfile
            , BytesSerializer<V> serializer, BytesDeserializer<V> deserializer
            ) {
        super(dbfile);
        this.serializer = serializer;
        this.deserializer = deserializer;
        raf = dbfile.randomAccessFile();
        if(dbfile.exists()) {
            try {
                this.size = raf.length();
            } catch(IOException e) {
                throw new TempDbException("Unable to read file lenagth from " + dbfile, e);
            }
        }
        
        os = new DbByteArrayOutputStream();
        try {
            dos = new DataOutputStream(os);
        } catch(Exception e) {
            throw new TempDbException("Unable to create temporary output stream from " + dbfile, e);
        }

        is = new DbByteArrayInputStream(new byte[1]);
        try {
            dis = new DataInputStream(is);
        } catch(Exception e) {
            throw new TempDbException("Unable to create temporary input stream from " + dbfile, e);
        }

    }

    @Override
    public void close() {
        try {
            raf.close();
        } catch(Exception e) {
            throw new TempDbException("Unable to close random access file from " + dbfile, e);
        }
    }

    /**
     * Add value to database
     * @param key
     * @param value 
     */
    public synchronized void put(K key, V value) {
        try {
            if(size != index) raf.seek(size);
            
            List<Long> ids = keymap.get(key);
            if(ids == null) {
                ids = new ArrayList<Long>(2);
                keymap.put(key, ids);
            }
            ids.add(size);
            
            os.reset();
            serializer.serialize(value, dos);
            int sz = os.count();

            raf.writeInt(sz);
            raf.write(os.buff(), 0, sz);
            size = size + 4 + sz;
            index = size;
            
        } catch(Exception e) {
            throw new TempDbException("Unable to write to random access file from " + dbfile, e);
        }
    }

    private V read(long id) throws IOException {
        if(id != index) raf.seek(id);
        int sz = raf.readInt();
        is.allocate(sz);
        int n = raf.read(is.buff(), 0, sz);
        is.count(n);
        V rv = deserializer.deserialize(dis);
        index = id + 4 + n;
        return rv;
    }

    /**
     * Reads values from database stored with key.
     * @param key
     * @return 
     */
    public synchronized List<V> get(K key) {
        try {
            List<V> rv = new ArrayList<V>();
            List<Long> list = keymap.get(key);
            if((list == null) || list.isEmpty()) return rv;
            for(Long long1 : list) {
                rv.add(read(long1));
            }
            return rv;
        } catch(Exception e) {
            throw new TempDbException("Unable to read to random access file from " + dbfile, e);
        }
    }

}
