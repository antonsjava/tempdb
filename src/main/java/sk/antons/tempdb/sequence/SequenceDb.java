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
package sk.antons.tempdb.sequence;

import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.RandomAccessFile;
import sk.antons.tempdb.TempDbException;
import sk.antons.tempdb.base.AbstractDb;
import sk.antons.tempdb.base.DbByteArrayInputStream;
import sk.antons.tempdb.base.DbByteArrayOutputStream;
import sk.antons.tempdb.base.DbFile;
import sk.antons.tempdb.serialization.BytesDeserializer;
import sk.antons.tempdb.serialization.BytesSerializer;

/**
 * Sequence (FIFO) type of database. Data are stored and read simultaneously.
 * @author antons
 */
public class SequenceDb<T> extends AbstractDb {
    protected BytesSerializer<T> serializer;
    protected BytesDeserializer<T> deserializer;
    protected RandomAccessFile raf;
    protected long index = 0;
    protected long size = 0;
    private DbByteArrayOutputStream os ;
    private DataOutputStream dos;
    private DbByteArrayInputStream is ;
    private DataInputStream dis;
    
    /**
     * Creates new database
     * @param dbfile
     * @param serializer 
     * @param deserializer 
     */
    public SequenceDb(DbFile dbfile, BytesSerializer<T> serializer, BytesDeserializer<T> deserializer) {
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
     * Add new value to end position
     * @param value 
     */
    public synchronized void add(T value) {
        try {
            os.reset();
            serializer.serialize(value, dos);
            raf.seek(size);
            int sz = os.size();
            raf.writeInt(sz);
            raf.write(os.buff(), 0, sz);
            size = size + sz + 4;
        } catch(Exception e) {
            throw new TempDbException("Unable to write to random access file from " + dbfile, e);
        }
    }
    
    /**
     * Reads next value from first unread position
     * @return value or null if no value exists
     */
    public synchronized T next() {
        if(index >= size) return null;
        try {
            raf.seek(index);
            int sz = raf.readInt();
            is.allocate(sz);
            int n = raf.read(is.buff(), 0, sz);
            is.count(n);
            index = index + n + 4;
            T rv = deserializer.deserialize(dis);
            return rv;
        } catch(Exception e) {
            throw new TempDbException("Unable to read random access file from " + dbfile, e);
        }
    }
}
