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
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyRecordReader;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom implementation/extension of the combine file input format designed to
 * combine small Avro files and skip any empty files or bad records.
 *
 * @param <T>	The Avro record type
 */
public class CombineAvroKeyFileInputFormat<T> extends CombineFileInputFormat<AvroKey<T>, NullWritable> {

	@Override
	protected List<FileStatus> listStatus(JobContext job) throws IOException {
		// Get the list from our parent...
		List<FileStatus> result = new ArrayList<>();

		// Loop through and remove any that are empty
		for (FileStatus file : super.listStatus(job)) {
			if (file.getLen() < 1) {
				logger().error("Skipping Empty file: " + file.getPath());
			}
			else {
				result.add(file);
			}
		}

		return result;
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public RecordReader<AvroKey<T>, NullWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException {
		return new CombineFileRecordReader((CombineFileSplit)split, context,
				DelegatingAvroRecordReader.class);
	}

	/**
	 *
	 * @param <T> Avro record type
	 */
	public static class DelegatingAvroRecordReader<T> extends RecordReader<AvroKey<T>, NullWritable> {

		private AvroKeyRecordReader<T> delegate;

		private final Integer splitIndex;

		public DelegatingAvroRecordReader(CombineFileSplit split,
				TaskAttemptContext context, Integer splitIndex) {
			Schema readerSchema = AvroJob.getInputKeySchema(context.getConfiguration());
			if (null == readerSchema) {
			  logger().warn("Reader schema was not set. Use AvroJob.setInputKeySchema() if desired.");
			  logger().info("Using a reader schema equal to the writer schema.");
			}
			this.splitIndex = splitIndex;
			delegate = new AvroKeyRecordReaderSkipBad<>(readerSchema);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return delegate.nextKeyValue();
		}

		@Override
		public AvroKey<T> getCurrentKey() throws IOException,
				InterruptedException {
			return delegate.getCurrentKey();
		}

		@Override
		public NullWritable getCurrentValue() throws IOException,
				InterruptedException {
			return NullWritable.get();
		}

		@Override
		public void initialize(InputSplit inputSplit, TaskAttemptContext context)
				throws IOException, InterruptedException {
			CombineFileSplit combineSplit = (CombineFileSplit)inputSplit;
			FileSplit split = new FileSplit(
					combineSplit.getPath(splitIndex),
					combineSplit.getOffset(splitIndex),
					combineSplit.getLength(splitIndex),
					combineSplit.getLocations());
			// Initialize with the single FileSplit for the current index
			delegate.initialize(split, context);
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return delegate.getProgress();
		}

		@Override
		public void close() throws IOException {
			delegate.close();
			delegate = null;
		}

		protected Logger logger() {
			return LoggerFactory.getLogger(this.getClass());
		}

	}

	/**
	 * This class is here to allow us to override the {@link AvroKeyRecordReader} with
	 * our own implementation that doesn't throw the "Invalid sync" exception.
	 *
	 *
	 * @param <T> Avro record type
	 */
	public static class AvroKeyRecordReaderSkipBad<T> extends AvroKeyRecordReader<T> {

		/** {@inheritDoc} */
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			boolean hasNext = false;
			try {
				hasNext = super.nextKeyValue();
			}
			catch (AvroRuntimeException are) {
				// Survive an "Invalid sync!" exception
				logger().error("Trapping " + are.getMessage());
			}
			return hasNext;
		}

		public AvroKeyRecordReaderSkipBad(Schema readerSchema) {
			super(readerSchema);
		}

		protected Logger logger() {
			return LoggerFactory.getLogger(this.getClass());
		}
	}

	protected Logger logger() {
		return LoggerFactory.getLogger(this.getClass());
	}

}
