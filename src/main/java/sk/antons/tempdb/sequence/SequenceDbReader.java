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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStream;
import sk.antons.tempdb.TempDbException;
import sk.antons.tempdb.base.AbstractDb;
import sk.antons.tempdb.base.DbFile;
import sk.antons.tempdb.serialization.BytesDeserializer;

/**
 * Reader of database file created by SequenceDbWriter.
 * @author antons
 */
public class SequenceDbReader<T> extends AbstractDb {
    protected BytesDeserializer<T> deserializer;
    protected InputStream is;
    protected DataInputStream dis;
    
    /**
     * Creates new database reader
     * @param dbfile
     * @param deserializer 
     */
    public SequenceDbReader(DbFile dbfile, BytesDeserializer<T> deserializer) {
        super(dbfile);
        dbfile.checkExistence();
        this.deserializer = deserializer;
        is = dbfile.inputStream();
        try {
            dis = new DataInputStream(is);
        } catch(Exception e) {
            throw new TempDbException("Unable to create input stream from " + dbfile, e);
        }
    }

    @Override
    public void close() {
        try {
            is.close();
        } catch(Exception e) {
            throw new TempDbException("Unable to close input stream from " + dbfile, e);
        }
    }



    /**
     * Reads next unread value from database.
     * @return next value or null
     */
    public synchronized T next() {
        try {
            T rv = deserializer.deserialize(dis);
            return rv;
        } catch(EOFException e) {
            return null;
        } catch(Exception e) {
            throw new TempDbException("Unable to read input stream from " + dbfile, e);
        }
    }
    
}
