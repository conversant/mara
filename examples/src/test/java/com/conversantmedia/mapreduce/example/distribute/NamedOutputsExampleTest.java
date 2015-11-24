package com.conversantmedia.mapreduce.example.distribute;

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


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import com.conversantmedia.mapreduce.example.NamedOutputsExample;
import com.conversantmedia.mapreduce.mrunit.MapReduceDriverTestBase;

/**
 *
 *
 */
public class NamedOutputsExampleTest
	extends MapReduceDriverTestBase<NamedOutputsExample,
		Text, LongWritable, Text, LongWritable, Text, LongWritable> {

	@Test @SuppressWarnings({ "unchecked", "rawtypes" })
	public void testNamedOutputs() {

		String lineStr = "a b c d e f a";

		List<Pair<LongWritable, Text>> inputs = new ArrayList<>();
		LongWritable offset = new LongWritable(0);
		Text line = new Text(lineStr);
		inputs.add(new Pair<>(offset, line));

		LongWritable ONE = new LongWritable(1l);
		List<Pair<Text,LongWritable>> outputs = new ArrayList<>();
		outputs.add(new Pair<>(new Text("a"), new LongWritable(2l)));
		outputs.add(new Pair<>(new Text("b"), ONE));
		outputs.add(new Pair<>(new Text("c"), ONE));
		outputs.add(new Pair<>(new Text("d"), ONE));
		outputs.add(new Pair<>(new Text("e"), ONE));
		outputs.add(new Pair<>(new Text("f"), ONE));

		MapReduceDriver driver = getTestDriver();
		driver.addAll(inputs);
		driver.addAllOutput(outputs);

		try {
			driver.runTest();

			// Check that our DEBUG line was written to the multiout
			verifyNamedOutput("DEBUG", new Text(lineStr.length() + ":"), line);

			// Example of how we can grab the (mock) MultipleOutput directly if needed
			MultipleOutputs multiOut = this.getNamedOutput();
			assertNotNull(multiOut);

		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}
}
