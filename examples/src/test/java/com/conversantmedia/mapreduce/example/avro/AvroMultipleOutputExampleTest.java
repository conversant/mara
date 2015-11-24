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


import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.conversantmedia.mapreduce.mrunit.ReduceDriverTestBase;
import com.dotomi.mapreduce.example.avro.AvroExample;

/**
 * Tests for the AvroMultipleOutputExample.
 *
 */
public class AvroMultipleOutputExampleTest extends
		ReduceDriverTestBase<AvroMultipleOutputExample,
			Text, LongWritable, AvroKey<AvroExample>, NullWritable> {

	@BeforeClass
	public static void setupHomeDir() {
		// to fake out winutils.exe
		System.setProperty("hadoop.home.dir", new File("src/test/resources/").getAbsolutePath());
	}
	
	@Test @SuppressWarnings({ "rawtypes", "unchecked" })
	public void testAvroMultipleOut() {
		
		ReduceDriver<Text, LongWritable, AvroKey<AvroExample>, NullWritable> driver = this.getTestDriver();
		LongWritable ONE = new LongWritable(1);
		List<String> words = new ArrayList<>();
		words.add("10");
		words.add("20");
		words.add("30");
		words.add("40");

		for (String word : words) {
			List<LongWritable> counts = new ArrayList<>();
			for (int i = 0; i < Long.parseLong(word); i++) {
				counts.add(ONE);
			}
			driver.addInput(new Text(word), counts);
		}

		try {
			List<Pair<AvroKey<AvroExample>, NullWritable>> results = driver.run();
			assertThat(results.size(), equalTo(words.size()));
			for (Pair<AvroKey<AvroExample>, NullWritable> e : results) {
				validateRecords(e.getFirst().datum());
			}

			AvroMultipleOutputs avroOut = this.getAvroNamedOutput();
			ArgumentCaptor<AvroKey> recordArg = ArgumentCaptor.forClass(AvroKey.class);
			try {
				verify(avroOut, times(words.size())).write(recordArg.capture(), eq(NullWritable.get()), eq("avroMulti"));
				for (AvroKey<AvroExample> record : recordArg.getAllValues()) {
					validateRecords(record.datum());
				}
			} catch (InterruptedException e) {
				fail(e.getMessage());
			}
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}

	private void validateRecords(AvroExample record) {
		assertThat(record.getFrequency(), equalTo(Long.parseLong((String) record.getWord())));
	}
}
