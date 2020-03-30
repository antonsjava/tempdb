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
import sk.antons.tempdb.serialization.BytesDeserializer;
import sk.antons.tempdb.serialization.BytesSerializer;
import sk.antons.tempdb.tree.AvlTreeDb;
import sk.antons.tempdb.tree.MapTreeDb;

/**
 * Map like database builder
 * @author antons
 */
public class TreeDbBuilder<K, V> {
    private DbFile dbfile;
    private BytesDeserializer<K> keydeserializer;
    private BytesSerializer<K> keyserializer;
    private BytesDeserializer<V> deserializer;
    private BytesSerializer<V> serializer;

    private TreeDbBuilder(Class<K> clazz, Class<V> clazz2) {}

    /**
     * Builder instantiator.
     * @param clazz key type
     * @param clazz2 value type
     * @return builder
     */
    public static <E, W> TreeDbBuilder<E, W> instance(Class<E> clazz, Class<W> clazz2) { return new TreeDbBuilder(clazz, clazz2); }

    /**
     * Sets data file
     * @param dbfile
     * @return this
     */
    public TreeDbBuilder<K, V> dbfile(DbFile dbfile) {
        this.dbfile = dbfile;
        return this;
    }
    
    /**
     * Sets data file
     * @param file
     * @return this
     */
    public TreeDbBuilder<K, V> file(File file) {
        this.dbfile = DbFile.instance(file);
        return this;
    }
    
    /**
     * Sets data file
     * @param filename
     * @return this
     */
    public TreeDbBuilder<K, V> file(String filename) {
        this.dbfile = DbFile.instance(filename);
        return this;
    }
    
    /**
     * Sets db file 
     * @param prefix prefix of temporary file
     * @param postfix postfix of temporary file
     * @param deleteOnExit true is file should be deleted on application exit
     * @return this
     */
    public TreeDbBuilder<K, V> tempfile(String prefix, String postfix, boolean deleteOnExit) {
        this.dbfile = DbFile.temporary(prefix, postfix, deleteOnExit);
        return this;
    }
    
    /**
     * Sets ket serializer
     * @param keyserializer
     * @return this
     */
    public TreeDbBuilder<K, V> keyserializer(BytesSerializer<K> keyserializer) {
        this.keyserializer = keyserializer;
        return this;
    }

    /**
     * Sets key deserializer
     * @param keydeserializer
     * @return this
     */
    public TreeDbBuilder<K, V> keydeserializer(BytesDeserializer<K> keydeserializer) {
        this.keydeserializer = keydeserializer;
        return this;
    }
    
    /**
     * Sets value serializer
     * @param serializer
     * @return this
     */
    public TreeDbBuilder<K, V> serializer(BytesSerializer<V> serializer) {
        this.serializer = serializer;
        return this;
    }

    /**
     * Sets value deserializer
     * @param deserializer
     * @return this
     */
    public TreeDbBuilder<K, V> deserializer(BytesDeserializer<V> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    /**
     * Creates avl database using dbfile, key and value serializer and deserializer.
     * @return database
     */
    public AvlTreeDb<K,V> avlTreeDb() {
        if(dbfile == null) throw new TempDbException("No dbfile defined fo new database");
        if(serializer == null) throw new TempDbException("No serializer defined fo new database");
        if(deserializer == null) throw new TempDbException("No deserializer defined fo new database");
        if(keyserializer == null) throw new TempDbException("No keyserializer defined fo new database");
        if(keydeserializer == null) throw new TempDbException("No keydeserializer defined fo new database");
        return new AvlTreeDb(dbfile, keyserializer, keydeserializer, serializer, deserializer);
    }
    
    /**
     * Creates avl database using dbfile, value serializer and deserializer.
     * @return 
     */
    public MapTreeDb<K,V> mapTreeDb() {
        if(dbfile == null) throw new TempDbException("No dbfile defined fo new database");
        if(serializer == null) throw new TempDbException("No serializer defined fo new database");
        if(deserializer == null) throw new TempDbException("No deserializer defined fo new database");
        return new MapTreeDb(dbfile, serializer, deserializer);
    }
}
