# tempdb

 Simple file based db for storing data too big to be stored in memory.

 Sometimes you are facing problem where you have large amount of data which you need 
 to process. Your processing is slow and you can't keep open connection to DB or 
 something similar. 

 This small java library allows you to temporary store such data in file on filesystem.

## Supported scenario
 
 There are two form of usage supported.
 
 - sequence usage - data are sequentially read and processed.You have long sequence of data which must be read from db and processed. As your processing is slow you can;t have open db connection whole time. So you can store that data to temporary file and then you can read them in same order as you store them and process it. 
 - find by key usage - data are stored with keys and then are look up by that keys. If you have lot of data, which you can't query by key each time when you need them you can make small key based db and then query if locally. (BigQuery is good example for such data transfer)

## Data storing 
 
 It is necessary to write code which converts data ti bytes and back. Library provides 
 only string storing. (I was using it for json strings.)
 But is is easy to write such code. Let say you have class
```java
public class Person {
	String name;
	int age;
}
```
Then de/serializers can be 
```java
public class PersonSerializer implements BytesSerializer<Person> {
    @Override
    public void serialize(Person value, DataOutputStream dos) throws IOException {
        if(value == null) throws new IOException("blabla");
        dos.writeUTF(Person.getName());
        dos.writeInt(Person.getAge());
    }
}

public class PersonDeserializer implements BytesDeserializer<Person> {
    @Override
    public Person deserialize(DataInputStream dis) throws IOException {
		Person p = new Person();
        p.setName(dis.readUTF());
        p.setAge(dis.readInt());
		return p;
    }
}
```


## Sequence usage

### Separated creation and usage
 
 Creation of data and their usage are clearly separated.

```java
// creation
SequenceDbWriter<String> writer = SequenceDbBuilder.instance(String.class)
    .tempfile("test", ".db", true)
    .serializer(StringSerialization.serializer())
    .sequenceDbWriter();

writer.add("jano");
writer.add(null);
writer.add("fero");
writer.close();

// usage
SequenceDbReader<String> reader = SequenceDbBuilder.instance(String.class)
    .dbfile(writer.dbfile()) // connection to creation phase
    .deserializer(StringSerialization.deserializer())
    .sequenceDbReader();

Assert.assertEquals("jano", reader.next());
Assert.assertEquals("", reader.next());
Assert.assertEquals("fero", reader.next());
Assert.assertNull(reader.next());
```
 
### Mixed creation and usage
 
 Creation of data and their usage are mixed. This scenario use useful only if you
 want to use db as buffer. 

```java
SequenceDb<String> db = SequenceDbBuilder.instance(String.class)
    .tempfile("test", ".db", true)
    .serializer(StringSerialization.serializer())
    .deserializer(StringSerialization.deserializer())
    .sequenceDb();

db.add("jano");
Assert.assertEquals("jano", db.next());
Assert.assertNull(db.next());
db.add(null);
Assert.assertEquals("", db.next());
Assert.assertNull(db.next());
db.add("ferowww");
Assert.assertEquals("ferowww", db.next());
Assert.assertNull(db.next());

db.close();
```

## Find by key usage

### Pure file storage

 In this case both data and keys are stored in file. It is safe for memory usage 
 but 'slow' for creating and reading file.
```java
AvlTreeDb<String, String> db = TreeDbBuilder.instance(String.class, String.class)
    .tempfile("test", ".db", true)
    .keyserializer(StringSerialization.serializer())
    .keydeserializer(StringSerialization.deserializer())
    .serializer(StringSerialization.serializer())
    .deserializer(StringSerialization.deserializer())
    .avlTreeDb();

db.put("jano", "jano");
db.put("ferowww", "ferowww");

Assert.assertEquals("jano", db.get("jano").get(0));
Assert.assertEquals("ferowww", db.get("ferowww").get(0));

db.close();
```


### Mixed memory/file storage
 
 In this case both data are stored in file and keys are stored in memory. It is faster for 
 for creating and reading file but can be problematic for memory usage. (But usually keys 
 are much smaller than data and in this case it is acceptable)

```java
MapTreeDb<String, String> db = TreeDbBuilder.instance(String.class, String.class)
    .tempfile("test", ".db", true)
    .serializer(StringSerialization.serializer())
    .deserializer(StringSerialization.deserializer())
    .mapTreeDb();

db.put("jano", "jano");
db.put("ferowww", "ferowww");

Assert.assertEquals("jano", db.get("jano").get(0));
Assert.assertEquals("ferowww", db.get("ferowww").get(0));

db.close();
```

## Maven usage

```
   <dependency>
      <groupId>com.github.antonsjava</groupId>
      <artifactId>tempdb</artifactId>
      <version>LASTVERSION</version>
   </dependency>
```
You can find LASTVERSION [here](https://mvnrepository.com/artifact/com.github.antonsjava/tempdb)
