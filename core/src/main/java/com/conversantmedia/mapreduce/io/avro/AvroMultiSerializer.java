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
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Serializer;

/**
 *
 *
 * @param <T> Avro record type
 */
public class AvroMultiSerializer<T>
	extends Configured implements Serializer<AvroMultiWrapper<T>> {

	private static final int AVRO_ENCODER_BLOCK_SIZE_BYTES = 512;

	/** An factory for creating Avro datum encoders. */
	private static final EncoderFactory ENCODER_FACTORY = new EncoderFactory().configureBlockSize(AVRO_ENCODER_BLOCK_SIZE_BYTES);

	private final Map<Class<T>, DatumWriter<T>> writers;

	private BinaryEncoder encoder;

	private OutputStream outputStream;

	public AvroMultiSerializer(Configuration conf) {
		super(conf);
		writers = new HashMap<>();
	}

	@Override @SuppressWarnings("unchecked")
	public void serialize(AvroMultiWrapper<T> avroWrapper) throws IOException {
		DatumWriter<T> writer = datumWriterFor((Class<T>) avroWrapper.datum().getClass());
		int b = MultiSchemaAvroSerialization.getIndexForSchema(getConf(), avroWrapper.datum().getClass());
		outputStream.write(b);
		writer.write(avroWrapper.datum(), encoder);
		this.encoder.flush();
	}

	private DatumWriter<T> datumWriterFor(Class<T> c) {
		DatumWriter<T> writer = writers.get(c);
		if (writer == null) {
			// Construct a new reader for this schema.
			writer = new ReflectDatumWriter<>(c);
			writers.put(c, writer);
		}
		return writer;
	}

	@Override
	public void open(OutputStream outputStream) throws IOException {
		this.outputStream = outputStream;
		this.encoder = ENCODER_FACTORY.binaryEncoder(outputStream, this.encoder);
	}

	@Override
	public void close() throws IOException {
		this.outputStream.close();
	}

}
