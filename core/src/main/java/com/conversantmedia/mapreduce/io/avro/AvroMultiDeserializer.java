package com.conversantmedia.mapreduce.io.avro;

/*
 * #%L
 * Mara Core framework
 * ~~
 * Copyright (C) 2015 Conversant
 * ~~
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;

/**
 *
 *
 * @param <T> Avro record type
 */
public class AvroMultiDeserializer<T> extends Configured implements
		Deserializer<AvroMultiWrapper<T>> {

	private Map<Integer, DatumReader<T>> readers;

	private BinaryDecoder decoder;

	public AvroMultiDeserializer(Configuration conf) {
		super(conf);
		readers = new HashMap<Integer, DatumReader<T>>();
	}

	@Override
	public AvroMultiWrapper<T> deserialize(AvroMultiWrapper<T> wrapper)
			throws IOException {
		if (wrapper == null) {
			wrapper = new AvroMultiWrapper<T>();
		}

		// Read in the first byte - the schema index
		int schemaIndex = decoder.inputStream().read();

		// Now hand off the rest to the datum reader for normal deser.
		DatumReader<T> reader = datumReaderFor(schemaIndex);
		wrapper.datum(reader.read(wrapper.datum(), decoder));
		return wrapper;
	}

	private DatumReader<T> datumReaderFor(int schemaIndex) {
		DatumReader<T> reader = readers.get(schemaIndex);
		if (reader == null) {
			Schema schema = MultiSchemaAvroSerialization.getSchemaAt(getConf(), schemaIndex);
			reader = new ReflectDatumReader<T>(schema);
			readers.put(schemaIndex, reader);
		}
		return reader;
	}

	@Override
	public void open(InputStream inputStream) throws IOException {
		this.decoder = DecoderFactory.get().directBinaryDecoder(inputStream, decoder);
	}

	@Override
	public void close() throws IOException {
		decoder.inputStream().close();
	}

}
