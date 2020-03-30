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
import sk.antons.tempdb.TempDbException;
import sk.antons.tempdb.base.AbstractDb;
import sk.antons.tempdb.base.DbByteArrayInputStream;
import sk.antons.tempdb.base.DbByteArrayOutputStream;
import sk.antons.tempdb.base.DbFile;
import sk.antons.tempdb.serialization.BytesDeserializer;
import sk.antons.tempdb.serialization.BytesSerializer;

/**
 * AVL map like database stored in file
 * @author antons
 */
public class AvlTreeDb<K, V> extends AbstractDb {
    protected BytesSerializer<K> keyserializer;
    protected BytesDeserializer<K> keydeserializer;
    protected BytesSerializer<V> serializer;
    protected BytesDeserializer<V> deserializer;
    protected RandomAccessFile raf;
    protected long index = 0;
    protected long size = 0;
    private DbByteArrayOutputStream keyos ;
    private DataOutputStream keydos;
    private DbByteArrayInputStream keyis ;
    private DataInputStream keydis;
    private DbByteArrayOutputStream os ;
    private DataOutputStream dos;
    private DbByteArrayInputStream is ;
    private DataInputStream dis;
    
    public AvlTreeDb(DbFile dbfile
            , BytesSerializer<K> keyserializer, BytesDeserializer<K> keydeserializer
            , BytesSerializer<V> serializer, BytesDeserializer<V> deserializer
            ) {
        super(dbfile);
        this.keyserializer = keyserializer;
        this.keydeserializer = keydeserializer;
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
        
        keyos = new DbByteArrayOutputStream();
        try {
            keydos = new DataOutputStream(keyos);
        } catch(Exception e) {
            throw new TempDbException("Unable to create temporary output stream from " + dbfile, e);
        }

        keyis = new DbByteArrayInputStream(new byte[1]);
        try {
            keydis = new DataInputStream(keyis);
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
     * add neq value to database
     * @param key
     * @param value 
     */    
    public synchronized void put(K key, V value) {
        try {
            boolean first = false;
            if(size == 0) {
                raf.writeLong(8l);
                size = 8;
                first = true;
            }
            
            Node node = new Node();
            node.id = size;
            
            int keysz = serializeKey(key);
            node.keySize = keysz;
            
            os.reset();
            serializer.serialize(value, dos);
            int sz = os.count();
            node.valueSize = sz;

            raf.seek(size);
            raf.writeLong(node.left);
            raf.writeLong(node.right);
            raf.writeLong(node.next);
            raf.writeInt(node.height);
            raf.writeInt(node.keySize);
            raf.writeInt(node.valueSize);
            raf.write(keyos.buff(), 0, keysz);
            raf.write(os.buff(), 0, sz);
            size = size + 8 + 8 + 8 + 4 + 4 + 4 + keysz + sz;
            
            if(first) return;
            long root = rootId();
            long newroot = insert(root, node.id, keydata());
            if(root != newroot) {
                raf.seek(0);
                raf.writeLong(newroot);
                rootId = newroot;
            }
            
        } catch(Exception e) {
            throw new TempDbException("Unable to write to random access file from " + dbfile, e);
        }
    }
    
    long rootId = -1;
    private long rootId() throws IOException {
        if(rootId > 0) return rootId;
        if(size <= 0) return 0;
        raf.seek(0);
        rootId = raf.readLong();
        return rootId;
    }

    /**
     * reads values from database identified by key
     * @param key
     * @return List of values
     */
    public synchronized List<V> get(K key) {
        List<V> list = new ArrayList<V>();
        if(size == 0) return list;
        try {

            int keysz = serializeKey(key);
            byte[] keydata = new byte[keysz];
            System.arraycopy(keyos.buff(), 0, keydata, 0, keysz);
            
            long root = rootId();
            
            Node node = findNode(root, keydata);
            if(node != null) node = loadNode(node.id, true, true);

            while(node != null) {
                list.add(bufferedValue());
                node = loadNode(node.next, true, true);
            }

            return list;
        } catch(Exception e) {
            throw new TempDbException("Unable to read random access file from " + dbfile, e);
        }
    }

    private int serializeKey(K key) throws IOException {
        keyos.reset();
        keyserializer.serialize(key, keydos);
        return keyos.count();
    }
    
    private byte[] keydata() throws IOException {
        byte[] rv = new byte[keyos.count()];
        System.arraycopy(keyos.buff(), 0, rv, 0, keyos.count());
        return rv;
    }
    
    private Node findNode(long id, byte[] keydata) throws IOException {
        Node node = loadNode(id, true, false);
        if(node == null) return null;
        int compare = compareKeyData(keyis.buff(), keyis.count(), keydata, keydata.length);
        if(compare == 0) {
            return node;
        } if(compare > 0) {
            return findNode(node.right, keydata);
        } else {
            return findNode(node.left, keydata);
        }
    }

    private static int compareKeyData(byte[] data1, int length1, byte[] data2, int length2) {
        if((data1 == null) && (data2 == null)) return 0;
        if(data1 == null) return -1;
        if(data2 == null) return 1;
        int size = length1;
        if(length2 < size) size = length2;
        for(int i = 0; i < size; i++) {
            if(data1[i] < data2[i]) return -1;       
            if(data1[i] > data2[i]) return 1;       
        }
        if(length1 < length2) return -1;
        if(length1 > length2) return 1;
        return 0;
    }

    
    private Node loadNode(long id, boolean loadKey, boolean loadValue) throws IOException {
        if(id <= 0) return null;
        raf.seek(id);
        Node node = new Node();
        node.id = id;
        node.left = raf.readLong();
        node.right = raf.readLong();
        node.next = raf.readLong();
        node.height = raf.readInt();
        node.keySize = raf.readInt();
        node.valueSize = raf.readInt();
        if(loadKey || loadValue) {
            keyis.allocate(node.keySize);
            int n = raf.read(keyis.buff(), 0, node.keySize);
            keyis.count(n);
        }
        if(loadValue) {
            is.allocate(node.valueSize);
            int n = raf.read(is.buff(), 0, node.valueSize);
            is.count(n);
        }
        return node;
    }

    private V bufferedValue() throws IOException {
        V rv = deserializer.deserialize(dis);
        return rv;
    }
    
    private K bufferedKey() throws IOException {
        K rv = keydeserializer.deserialize(keydis);
        return rv;
    }

    private void saveNode(Node node) throws IOException {
        if(node == null) return;
        if(node.id <= 0) return;
        raf.seek(node.id);
        raf.writeLong(node.left);
        raf.writeLong(node.right);
        raf.writeLong(node.next);
        raf.writeInt(node.height);
    }


    private long insert(long id, long newId, byte[] keydata) throws IOException {
        if(id <= 0) return newId;
        Node node = loadNode(id, true, false);
        if(node == null) throw new IllegalStateException("Unknown address " + id);
        int compare = compareKeyData(keyis.buff(), keyis.count(), keydata, keydata.length);
        if(compare == 0) {
            while(node.next > 0) {
                node = loadNode(node.next, false, false);
            }
            node.next = newId;
            saveNode(node);
            return id;
        } if(compare > 0) {
            node.right = insert(node.right, newId, keydata);
        } else {
            node.left = insert(node.left, newId, keydata);
        }
        saveNode(node);
        Node n = rebalance(node);
        return n.id;
    }


    private Node rebalance(Node node) throws IOException {
        updateHeight(node);
        int balance = getBalance(node);
        if (balance > 1) {
            Node right = loadNode(node.right, false, false);
            Node rightright = loadNode(right.right, false, false);
            Node rightleft = loadNode(right.left, false, false);
            if (height(rightright) > height(rightleft)) {
                node = rotateLeft(node);
            } else {
                node.right = rotateRight(right).id;
                saveNode(node);
                node = rotateLeft(node);
            }
        } else if (balance < -1) {
            Node left = loadNode(node.left, false, false);
            Node leftright = loadNode(left.right, false, false);
            Node leftleft = loadNode(left.left, false, false);
            if (height(leftleft) > height(leftright))
                node = rotateRight(node);
            else {
                node.left = rotateLeft(left).id;
                saveNode(node);
                node = rotateRight(node);
            }
        }
        return node;
    }

    private Node rotateLeft(Node node) throws IOException {
        Node right = loadNode(node.right, false, false);
        Node righleft = loadNode(right.left, false, false);
        right.left = (node == null)? 0 : node.id;
        node.right = (righleft == null)? 0 : righleft.id;
        updateHeight(node);
        updateHeight(right);
        saveNode(right);
        saveNode(node);
        return right;
    }


    private Node rotateRight(Node node) throws IOException {
        Node left = loadNode(node.left, false, false);
        Node leftright = loadNode(left.right, false, false);
        left.right = (node == null)? 0 : node.id;
        node.left = (leftright == null)? 0 : leftright.id;
        updateHeight(node);
        updateHeight(left);
        saveNode(left);
        saveNode(node);
        return left;
    }

    private void updateHeight(Node node) throws IOException {
        if(node == null) return;
        int oldval = node.height;
        Node right = loadNode(node.right, false, false);
        Node left = loadNode(node.left, false, false);
        node.height = Math.max(height(right), height(left)) + 1;
        if(node.height != oldval) saveNode(node);
    }
 
    private int height(Node node) {
        if(node == null) return 0;
        return node.height;
    }
 
    private int getBalance(Node node) throws IOException {
        if(node == null) return 0;
        Node right = loadNode(node.right, false, false);
        Node left = loadNode(node.left, false, false);
        return height(right) - height(left);
    }
 
    public String dump() {
        try {
            if(size <= 0) return "EMPTY";
            StringBuilder sb = new StringBuilder();
            long root = rootId();
            if(root <= 0) return "EMPTY";
            dump(root, "", sb);
            return sb.toString();
        } catch(Exception e) {
            e.printStackTrace();
            return e.toString();
        }
        
    }
    
    public void dump(long id, String prefix, StringBuilder sb) throws IOException {
        if(id <= 0) return;
        Node node = loadNode(id, true, false);
        if(node == null) return;
        K key = bufferedKey();
        sb.append(prefix).append(key);
        sb.append(" id: ").append(node.id);
        sb.append(" left: ").append(node.left);
        sb.append(" right: ").append(node.left);
        sb.append(" next: ").append(node.next);
        sb.append(" height: ").append(node.height);
        sb.append("\n");
        //dump(node.next, prefix+ "|  ", sb);
        dump(node.left, prefix+ "|  ", sb);
        dump(node.right, prefix+"|  ", sb);
    }

    private static class Node {
        protected long id;    
        protected long left;  
        protected long right;  
        protected long next;
        protected int height;
        protected int keySize;
        protected int valueSize;

        @Override
        public String toString() {
            return "Node{" + "id=" + id + ", left=" + left + ", right=" + right + ", next=" + next + ", height=" + height + ", keySize=" + keySize + ", valueSize=" + valueSize + '}';
        }

    
    }
}
