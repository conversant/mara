package com.conversantmedia.mapreduce.output;

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


import static com.google.common.base.Preconditions.checkState;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Output fomat for writing bloom filters. This is a simple wrapper
 * around the Dotomi StringBloomFilter.
 *
 * @param <K> Key
 * @param <V> Value
 */
public class BloomFilterOutputFormat<K, V> extends TextOutputFormat<K, V> {

	public static final String CONF_KEY_EXPECTED_INSERTIONS = "dotomi.bloomfilter.insertions";

	public static final String BLOOM_FILTER = "bloomfilter";

	private RecordWriter<K,V> writer;

	private TaskAttemptContext context;

	private int insertionSize;

	public BloomFilterOutputFormat() {}

	public BloomFilterOutputFormat(TaskAttemptContext context, int size) {
		this.context = context;
		this.insertionSize = size;
		context.getConfiguration().set(BASE_OUTPUT_NAME, BLOOM_FILTER);
	}

	public BloomFilterOutputFormat(TaskAttemptContext context, int size, String fileNamePrefix) {
		this.context = context;
		this.insertionSize = size;
		context.getConfiguration().set(BASE_OUTPUT_NAME, fileNamePrefix);
	}

	/**
	 *
	 * @param key						the key to write
	 * @param value						the value to write
	 * @throws IOException				thrown if writing fails
	 * @throws InterruptedException		thrown if writing thread is interrupted
	 */
	public void write(K key, V value) throws IOException, InterruptedException {
		checkState(this.context != null);
		getRecordWriter(context).write(key, value);
	}

	/**
	 *
	 * @param <K>	key type for the bloom filter
	 * @param <V>	value type for the bloom filter
	 */
	protected static class BloomFilterRecordWriter<K, V> extends RecordWriter<K,V> {

		private final StringBloomFilter bloomFilter;

		protected final DataOutputStream out;

		public BloomFilterRecordWriter(DataOutputStream out, int expected) {
			this.out = out;
			this.bloomFilter = new StringBloomFilter();
			bloomFilter.init(expected);
		}

		@Override
		public void write(K key, V value) throws IOException,
				InterruptedException {
			this.bloomFilter.addToFilter(value.toString());
		}

		@Override
		public synchronized void close(TaskAttemptContext context)
				throws IOException, InterruptedException {
			this.bloomFilter.serializeFilter(new ObjectOutputStream(out));
			out.close();
		}
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		if (writer == null) {
			int size = getExpectedInsertions(job);
			checkState(size > 0, "Expected insertion insertionSize not set.");

			Configuration conf = job.getConfiguration();
			String extension = "";
			Path file = getDefaultWorkFile(job, extension);
			FileSystem fs = file.getFileSystem(conf);
			FSDataOutputStream fileOut = fs.create(file, false);

			writer = new BloomFilterRecordWriter<>(fileOut, size);
		}
		return writer;
	}

	protected int getExpectedInsertions(TaskAttemptContext job) {
		return this.insertionSize > 0? this.insertionSize : job.getConfiguration().getInt(CONF_KEY_EXPECTED_INSERTIONS, -1);
	}

	/**
	 * Sets the number of insertions expected for our Bloom filter to
	 * ensure it's adequately sized.
	 * @param job	the context
	 * @param size	the size to set
	 */
	public static void setExpectedInsertions(JobContext job, int size) {
		job.getConfiguration().setInt(CONF_KEY_EXPECTED_INSERTIONS, size);
	}

	/**
	 *
	 * @param job					the context
	 * @throws InterruptedException	thrown if the write is interrupted
	 * @throws IOException			thrown if the write fails
	 */
	public void close(TaskAttemptContext job) throws IOException, InterruptedException {
		if (this.writer != null) {
			this.writer.close(job);
		}
	}
}
