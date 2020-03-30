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
package sk.antons.tempdb.serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Implementation of string serialization. Null is serialized as empty string.
 * @author antons
 */
public class StringSerialization {

    private static final Serializer serializer = new Serializer();
    public static Serializer serializer() { return serializer; }

    private static final Deserializer deserializer = new Deserializer();
    public static Deserializer deserializer() { return deserializer; };

    public static class Serializer implements BytesSerializer<String> {

        @Override
        public void serialize(String value, DataOutputStream dos) throws IOException {
            if(value == null) value = "";
            dos.writeUTF(value);
        }
    
    }
    
    public static class Deserializer implements BytesDeserializer<String> {

        @Override
        public String deserialize(DataInputStream dis) throws IOException {
            return dis.readUTF();
        }

    
    }
    
}
