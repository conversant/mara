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


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;

/**
 * Common methods for handling BloomFilters.
 */
public abstract class AbstractBloomFilter {

	@SuppressWarnings("rawtypes")
	protected BloomFilter filter;

	/**
	 * This is 1%, not 0.01%.
	 */
	protected static final double DEFAULT_FALSE_POSSITIVE = 0.01d;


	/**
	 * In order to support serialization the funnel must be serializable.
	 * @param expectedInsertions	number of insertions expected
	 * @param falsePossitiveRate	the desired false positive probability (must be positive and
	 *        less than 1.0)
	 * @param funnel				the funnel of T's that the constructed {@code BloomFilter<T>} will use
	 * @see com.google.common.hash.BloomFilter#create(Funnel, int, double)
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void init(final int expectedInsertions, final double falsePossitiveRate, final Funnel funnel)
	{
		assert funnel instanceof java.io.Serializable;
		filter = BloomFilter.create(funnel, expectedInsertions, falsePossitiveRate);
	}


	/**
	 * Serializes the wrapped BloomFilter
	 * @param output		destination for serialization
	 * @throws IOException	thrown if IO fails
	 */
	public void serializeFilter(final ObjectOutputStream output) throws IOException
	{
		output.writeObject(filter);
	}

	/**
	 * Deserializes the BloomFilter into the current wrapper
	 * @param input						destination for deserialization
	 * @throws IOException				thrown if reading fails
	 * @throws ClassNotFoundException	thrown if unable to locate appropriate class
	 */
	@SuppressWarnings("rawtypes")
	public void deserializeFilter(final ObjectInputStream input) throws IOException, ClassNotFoundException
	{
		filter = (BloomFilter)input.readObject();
	}

}
