package com.conversantmedia.mapreduce.example.avro;

/*
 * #%L
 * Mara Framework Examples
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


import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

/**
 * Utilities for unit testing with Avro records.
 *
 */
public class AvroUnitTestHelper {

	private static AvroUnitTestHelper instance;

	private AvroUnitTestHelper() {}

	public static AvroUnitTestHelper getInstance() {
		if (instance == null) {
			instance = new AvroUnitTestHelper();
		}
		return instance;
	}

	public <D> Iterable<D> getRecords(String path) throws IOException {
		return getRecords(new File(path));
	}

	public <D> Iterable<D> getRecords(File file) throws IOException {
		Schema schema = getSchema(file);
		DatumReader<D> reader = new SpecificDatumReader<>(schema);
		FileReader<D> fileReader = DataFileReader.openReader(file, reader);
		return fileReader;
	}

	/**
	 *
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public Schema getSchema(File file) throws IOException {
		Schema schema = null;
		FileReader<IndexedRecord> fileReader = null;
		try {
			DatumReader<IndexedRecord> reader = new GenericDatumReader<>();
			fileReader = DataFileReader.openReader(file, reader);
			schema = fileReader.getSchema();
		}
		finally {
			if (fileReader != null) {
				fileReader.close();
			}
		}
		return schema;
	}

}
