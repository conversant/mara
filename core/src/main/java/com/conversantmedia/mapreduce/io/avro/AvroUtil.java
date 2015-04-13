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


import java.lang.reflect.Field;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import com.conversantmedia.mapreduce.tool.ToolException;

/**
 *
 *
 */
public class AvroUtil {

	/**
	 *
	 * @param recordClass		Avro record type class
	 * @return					the Avro schema for the supplied class
	 * @throws ToolException	if an exception is thrown extracting the schema from the record class
	 */
	public static Schema getSchema(Class<? extends SpecificRecordBase> recordClass) throws ToolException {
		try {
			// Will have a static member SCHEMA$ from which we can parse the schema:
			Field field = recordClass.getDeclaredField("SCHEMA$");
			return (Schema)field.get(null);
		}
		catch (NoSuchFieldException | SecurityException
				| IllegalArgumentException | IllegalAccessException e) {
			throw new ToolException(e);
		}
	}

}
