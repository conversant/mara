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


import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.BeforeClass;
import org.junit.Test;

import com.conversantmedia.mapreduce.mrunit.MapDriverTestBase;
import com.dotomi.mapreduce.example.avro.AvroExample;

/**
 *
 *
 */
public class AvroInputOutputMapperTest extends
		MapDriverTestBase<AvroInputOutputExample,
			AvroKey<AvroExample>, NullWritable, AvroKey<AvroExample>, NullWritable> {

	@BeforeClass
	public static void setupHomeDir() {
		// to fake out winutils.exe
		System.setProperty("hadoop.home.dir", new File("src/test/resources/").getAbsolutePath());
	}
	
	public String getMinimum_testShakespeareThe() {
		return "23436"; // should get only 1 - 'the'
	}

	@Test
	public void testShakespeareThe() {
		MapDriver<AvroKey<AvroExample>, NullWritable, AvroKey<AvroExample>, NullWritable> driver =
				this.getTestDriver();

		AvroUnitTestHelper helper = AvroUnitTestHelper.getInstance();
		Iterable<AvroExample> records;
		try {
			records = helper.getRecords("src/test/resources/shakespeare_counts.avro");
			for (AvroExample record : records) {
				driver.addInput(new Pair<AvroKey<AvroExample>, NullWritable>(new AvroKey<AvroExample>(record), NullWritable.get()));
			}

			AvroExample.Builder builder = AvroExample.newBuilder();
			builder.setWord("the");
			builder.setFrequency(23437l);
			AvroKey<AvroExample> avroKey = new AvroKey<AvroExample>(builder.build());
			driver.addOutput(new Pair<AvroKey<AvroExample>, NullWritable>(avroKey, NullWritable.get()));

			driver.runTest();
		}
		catch (IOException e) {
			fail(e.getMessage());
		}


	}
}
