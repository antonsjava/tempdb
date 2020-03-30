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
package sk.antons.tempdb.base;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import sk.antons.tempdb.TempDbException;

/**
 * Database file helper class
 * @author antons
 */
public class DbFile {
    
    private File file;
    
    /**
     * Constructs db file using specified filesystem file.
     * @param file 
     */
    public DbFile(File file) {
        this.file = file;
        check();
    }

    /**
     * Constructs db file using specified filesystem file.
     * @param file 
     */
    public static DbFile instance(File file) {
        return new DbFile(file);
    }

    /**
     * Constructs db file using specified filesystem file.
     * @param filename 
     */
    public static DbFile instance(String filename) {
        return new DbFile(new File(filename));
    }
    
    /**
     * Constructs db file using new temporary file.
     * @param prefix 
     * @param postfix 
     * @param deleteOnExit 
     */
    public static DbFile temporary(String prefix, String postfix, boolean deleteOnExit) {
        try {
            File f = File.createTempFile(prefix, prefix);
            if(deleteOnExit) f.deleteOnExit();
            return new DbFile(f);
        } catch(Exception e) {
            throw new TempDbException("Unable to create temp database file", e);
        }
    }
    
    private void check() {
        if(file == null) throw new TempDbException("Null db file");
        if(file.exists()) return;
        try {
            File parent = file.getParentFile();
            if(!parent.exists()) parent.mkdirs();
        } catch(Exception e) {
            throw new TempDbException("Unable to create parent folder for " + file, e);
        }
    }
    
    /**
     * Check if file already exists
     * @return 
     */
    public boolean exists() {
        return file.exists();
    }

    /**
     * Check if file already exists
     * Throws exception if file not exists 
     */
    public void checkExistence() {
        if(file.exists()) return;
        throw new TempDbException("Database file not exists '" + file + "'");
    }
    
    /**
     * Deletes file
     */
    public void delete() {
        file.delete();
    }

    /**
     * Creates input stream from file
     * @return InputStream
     */
    public InputStream inputStream() {
        try {
            return new BufferedInputStream(new FileInputStream(file), 20000);
        } catch(Exception e) {
            throw new TempDbException("Unable to create input stream from '" + file + "'");
        }
    }
    
    /**
     * Creates output stream from file
     * @return OutputStream
     */
    public OutputStream outputStream() {
        try {
            return new BufferedOutputStream(new FileOutputStream(file), 20000);
        } catch(Exception e) {
            throw new TempDbException("Unable to create output stream from '" + file + "'");
        }
    }
    
    /**
     * Creates random access file from file
     * @return RandomAccessFile
     */
    public RandomAccessFile randomAccessFile() {
        try {
            return new RandomAccessFile(file, "rw");
        } catch(Exception e) {
            throw new TempDbException("Unable to create random access file from '" + file + "'");
        }
    }

    @Override
    public String toString() {
        return "DbFile{" + file + '}';
    }


}
