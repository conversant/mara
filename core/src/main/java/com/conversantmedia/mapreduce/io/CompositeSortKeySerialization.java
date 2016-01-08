package com.conversantmedia.mapreduce.io;

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


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.Job;

/**
 * Encapsulates the serialization functionality for a job 
 * using the {@link CompositeSortKey} key.
 * 
 * @param <G> Grouping/Partitioning key type
 * @param <S> Sorting key type
 */
public class CompositeSortKeySerialization<G extends WritableComparable<G>, S extends WritableComparable<S>>
	extends Configured implements Serialization<CompositeSortKey<G, S>> {

	public static final String CONF_KEY_GROUPKEY_CLASS = "com.conversantmedia.mapreduce.io.compositekey.groupclass";
	public static final String CONF_KEY_SORTKEY_CLASS = "com.conversantmedia.mapreduce.io.compositekey.sortclass";

	private Serializer<CompositeSortKey<G, S>> serializer;
	private Deserializer<CompositeSortKey<G, S>> deserializer;

	public CompositeSortKeySerialization() {}

	public CompositeSortKeySerialization(Configuration conf) {
		super(conf);
	}

	@Override
	public boolean accept(Class<?> c) {
		return CompositeSortKey.class.isAssignableFrom(c);
	}

	@Override @SuppressWarnings({ "unchecked", "rawtypes" })
	public Deserializer<CompositeSortKey<G, S>> getDeserializer(
			Class<CompositeSortKey<G, S>> arg0) {
		if (deserializer == null) {
			deserializer = new CompositeSortKeyDeserializer(getConf().getClass(CONF_KEY_GROUPKEY_CLASS, null),
					getConf().getClass(CONF_KEY_SORTKEY_CLASS, null));
		}
		return deserializer;
	}

	@Override
	public Serializer<CompositeSortKey<G, S>> getSerializer(
			Class<CompositeSortKey<G, S>> arg0) {
		if (serializer == null) {
			serializer = new CompositeSortKeySerializer<>();
		}
		return serializer;
	}

	/**
	 * Convenience method to configure the job for using the composite key.
	 * @param job				the job using this serializer
	 * @param groupKeyClass		the key type used for grouping
	 * @param sortKeyClass		the key type used for sorting
	 */
	@SuppressWarnings("rawtypes")
	public static void configureMapOutputKey(Job job, Class<? extends WritableComparable> groupKeyClass,
			Class<? extends WritableComparable> sortKeyClass) {

		// First, setup our classes...
		job.getConfiguration().set(CONF_KEY_GROUPKEY_CLASS, groupKeyClass.getName());
		job.getConfiguration().set(CONF_KEY_SORTKEY_CLASS, sortKeyClass.getName());

		// Set this class as our map output key
		job.setMapOutputKeyClass(CompositeSortKey.class);

		// Setup the partitioner and comparators.
		job.setPartitionerClass(CompositeSortKey.KeyPartitioner.class);
		job.setGroupingComparatorClass(CompositeSortKey.GroupingComparator.class);
		job.setSortComparatorClass(CompositeSortKey.NaturalSortComparator.class);

		// Now setup the serialization by registering with the framework.
		Collection<String> serializations = new ArrayList<>();
		serializations.add(CompositeSortKeySerialization.class.getName());
		serializations.addAll(job.getConfiguration().getStringCollection("io.serializations"));
		job.getConfiguration().setStrings("io.serializations", serializations.toArray(new String[serializations.size()]));

	}

	/**
	 * Handles serialization of the composite sort keys.
	 *
	 * @param <G> Grouping/Partitioning key type
	 * @param <S> Sorting key type
	 */
	public static final class CompositeSortKeySerializer<G extends WritableComparable<G>, S extends WritableComparable<S>>
		implements Serializer<CompositeSortKey<G, S>> {

		private DataOutputStream out;

		@Override
		public void serialize(CompositeSortKey<G, S> key) throws IOException {
			key.getGroupKey().write(this.out);
			key.getSortKey().write(this.out);
		}

		@Override
		public void open(OutputStream out) throws IOException {
			this.out = new DataOutputStream(out);
		}

		@Override
		public void close() throws IOException {
			IOUtils.closeStream(this.out);
		}
	}

	/**
	 * Handles deserialization of the sort keys.
	 *
	 * @param <G> Grouping/Partitioning key type
	 * @param <S> Sorting key type
	 */
	public static final class CompositeSortKeyDeserializer<G extends WritableComparable<G>, S extends WritableComparable<S>>
			implements Deserializer<CompositeSortKey<G, S>> {

		private DataInputStream in;

		private final Class<G> groupKeyClass;
		private final Class<S> sortKeyClass;

		public CompositeSortKeyDeserializer(Class<G> groupKeyClass, Class<S> sortKeyClass) {
			this.groupKeyClass = groupKeyClass;
			this.sortKeyClass = sortKeyClass;
		}

		@Override
		public CompositeSortKey<G, S> deserialize(CompositeSortKey<G, S> reuse)
				throws IOException {
			if (reuse == null) {
				reuse = new CompositeSortKey<>();
			}

			if (reuse.getGroupKey() == null) {
				try {
					reuse.setGroupKey(groupKeyClass.newInstance());
				} catch (InstantiationException | IllegalAccessException e) {
					throw new IOException("Unable to instantiate '" + groupKeyClass + "'");
				}
			}

			if (reuse.getSortKey() == null) {
				try {
					reuse.setSortKey(sortKeyClass.newInstance());
				} catch (InstantiationException | IllegalAccessException e) {
					throw new IOException("Unable to instantiate '" + sortKeyClass + "'");
				}
			}

			// Use the keys to deserialize...
			reuse.getGroupKey().readFields(this.in);
			reuse.getSortKey().readFields(this.in);

			return reuse;
		}

		@Override
		public void open(InputStream in) throws IOException {
			this.in = new DataInputStream(in);
		}

		@Override
		public void close() throws IOException {
			IOUtils.closeStream(this.in);
		}
	}
}
