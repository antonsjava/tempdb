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

import java.io.DataOutputStream;
import java.io.OutputStream;
import sk.antons.tempdb.TempDbException;
import sk.antons.tempdb.base.AbstractDb;
import sk.antons.tempdb.base.DbFile;
import sk.antons.tempdb.serialization.BytesSerializer;

/**
 * Writer for database file 
 * Such file can be then used by SequenceDbReader.
 * @author antons
 */
public class SequenceDbWriter<T> extends AbstractDb {
    protected BytesSerializer<T> serializer;
    protected OutputStream os;
    protected DataOutputStream dos;
    
    public SequenceDbWriter(DbFile dbfile, BytesSerializer<T> serializer) {
        super(dbfile);
        this.serializer = serializer;
        os = dbfile.outputStream();
        try {
            dos = new DataOutputStream(os);
        } catch(Exception e) {
            throw new TempDbException("Unable to create output stream from " + dbfile, e);
        }
    }

    @Override
    public void close() {
        try {
            dos.flush();
            os.flush();
            os.close();
        } catch(Exception e) {
            throw new TempDbException("Unable to close output stream from " + dbfile, e);
        }
    }
    
    /**
     * Add next value to the database
     * @param value 
     */
    public synchronized void add(T value) {
        try {
            serializer.serialize(value, dos);
        } catch(Exception e) {
            throw new TempDbException("Unable to write to output stream from " + dbfile, e);
        }
    }
    
}
