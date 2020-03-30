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

/**
 * Helper class for database implementation
 * @author antons
 */
public abstract class AbstractDb {
    protected DbFile dbfile;

    public AbstractDb(DbFile dbfile) {
        this.dbfile = dbfile;
    }

    /**
     * Returns db file used by this database
     * @return dbfile
     */
    public DbFile dbfile() { return dbfile; }

    /**
     * Closes resources used by this db.
     */
    public abstract void close();

    /**
     * Deletes real file used by this db
     */
    public void delete() {
        dbfile.delete();
    }
}
