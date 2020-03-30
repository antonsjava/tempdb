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
package sk.antons.tempdb;

import java.io.File;
import sk.antons.tempdb.base.DbFile;
import sk.antons.tempdb.sequence.SequenceDb;
import sk.antons.tempdb.sequence.SequenceDbReader;
import sk.antons.tempdb.sequence.SequenceDbWriter;
import sk.antons.tempdb.serialization.BytesDeserializer;
import sk.antons.tempdb.serialization.BytesSerializer;

/**
 * Builder for sequence type DB.
 *
 * @author antons
 */
public class SequenceDbBuilder<V> {
    private DbFile dbfile;
    private BytesDeserializer<V> deserializer;
    private BytesSerializer<V> serializer;

    private SequenceDbBuilder(Class<V> clazz) {}

    /**
     * Builder instantiator
     * @param clazz type of resulted db
     * @return 
     */
    public static <E> SequenceDbBuilder<E> instance(Class<E> clazz) { return new SequenceDbBuilder(clazz); }
    
    /**
     * Sets base db file
     * @param dbfile
     * @return this
     */
    public SequenceDbBuilder<V> dbfile(DbFile dbfile) {
        this.dbfile = dbfile;
        return this;
    }
    
    /**
     * Sets base db file
     * @param file
     * @return this
     */
    public SequenceDbBuilder<V> file(File file) {
        this.dbfile = DbFile.instance(file);
        return this;
    }
    
    /**
     * Sets db file
     * @param filename
     * @return this
     */
    public SequenceDbBuilder<V> file(String filename) {
        this.dbfile = DbFile.instance(filename);
        return this;
    }
    
    /**
     * Sets db file
     * @param prefix prefix of temporary file
     * @param postfix postfix if temporary file
     * @param deleteOnExit true if delete file on application exit
     * @return this
     */
    public SequenceDbBuilder<V> tempfile(String prefix, String postfix, boolean deleteOnExit) {
        this.dbfile = DbFile.temporary(prefix, postfix, deleteOnExit);
        return this;
    }
    
    /**
     * Sets serializer for valius
     * @param serializer
     * @return this
     */
    public SequenceDbBuilder<V> serializer(BytesSerializer<V> serializer) {
        this.serializer = serializer;
        return this;
    }
    
    /**
     * Sets deserializer for values
     * @param deserializer
     * @return this
     */
    public SequenceDbBuilder<V> deserializer(BytesDeserializer<V> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    /**
     * Create sequence db reader using dbfile and deserializer.
     * @return this
     */
    public SequenceDbReader<V> sequenceDbReader() {
        if(dbfile == null) throw new TempDbException("No dbfile defined fo new database");
        if(deserializer == null) throw new TempDbException("No deserializer defined fo new database");
        return new SequenceDbReader(dbfile, deserializer);
    }
    
    /**
     * Create sequence db writer using dbfile and serializer.
     * @return database
     */
    public SequenceDbWriter<V> sequenceDbWriter() {
        if(dbfile == null) throw new TempDbException("No dbfile defined fo new database");
        if(serializer == null) throw new TempDbException("No serializer defined fo new database");
        return new SequenceDbWriter(dbfile, serializer);
    }
    
    /**
     * Create sequence db using dbfile, serializer and deserializer.
     * @return databese
     */
    public SequenceDb<V> sequenceDb() {
        if(dbfile == null) throw new TempDbException("No dbfile defined fo new database");
        if(serializer == null) throw new TempDbException("No serializer defined fo new database");
        if(deserializer == null) throw new TempDbException("No deserializer defined fo new database");
        return new SequenceDb(dbfile, serializer, deserializer);
    }
}
