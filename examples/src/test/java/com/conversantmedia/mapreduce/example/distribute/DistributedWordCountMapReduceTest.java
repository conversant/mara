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


import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import com.conversantmedia.mapreduce.example.distribute.DistributedObjectExample;
import com.conversantmedia.mapreduce.mrunit.MapReduceDriverTestBase;

public class DistributedWordCountMapReduceTest
	extends MapReduceDriverTestBase<DistributedObjectExample,
	LongWritable, Text, Text, LongWritable, Text, LongWritable> {

	@SuppressWarnings("unused")
	private Set<String> getBlacklist_runMyTest() {
		Set<String> words = new HashSet<String>();
		words.add("RunMyTest");
		return words;
	}

	protected Integer getMinimum_runMyTest() {
		return 100;
	}

	public Integer getMinimum() {
		return 1;
	}

	@Test @SuppressWarnings({ "rawtypes", "unchecked" })
	public void runMyTest() {

		List<Pair<LongWritable, Text>> inputs = new ArrayList<Pair<LongWritable, Text>>();
		inputs.add(new Pair<LongWritable, Text>(
				new LongWritable(1), new Text("the quick brown fox jumped over the lazy dog.")));

		MapReduceDriver driver = getTestDriver();
		driver.addAll(inputs);

		try {
			driver.runTest();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
