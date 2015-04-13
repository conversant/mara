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


import java.io.BufferedReader;
import java.io.IOException;
import java.util.Set;

import org.slf4j.LoggerFactory;

import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;

/**
 * COPIED from dcs-public to avoid adding dependency on library
 * for relatively minor class.
 *
 *  BloomFilter wrapper to make String operations easier
 */
public class StringBloomFilter extends AbstractBloomFilter {

	private static final org.slf4j.Logger log = LoggerFactory.getLogger(StringBloomFilter.class);

	public void init(final int expectedInsertions)
	{
		init(expectedInsertions, DEFAULT_FALSE_POSSITIVE);
	}

	public void init(final int expectedInsertions, final double falsePossitiveRate)
	{
		//All funnels created by Funnels factory are serializable
		Funnel<byte[]> byteFunnel = Funnels.byteArrayFunnel();
		init(expectedInsertions, falsePossitiveRate, byteFunnel);
	}


	@SuppressWarnings("unchecked")
	public boolean mightContain(final String check)
	{
		return filter.mightContain(check.getBytes());
	}

	@SuppressWarnings("unchecked")
	public boolean mightContain(final byte[] check)
	{
		return filter.mightContain(check);
	}

	@SuppressWarnings("unchecked")
	public void addToFilter(final String s)
	{
		filter.put(s.getBytes());
	}

	public void addToFilter(final String[] s)
	{
		for (String t : s)
		{
			addToFilter(t);
		}
	}

	public void addToFilter(final Set<String> s)
	{
		for (String t : s)
		{
			addToFilter(t);
		}
	}

	public void addToFilter(final BufferedReader input)
	{
		try {
			while (input.ready())
			{
				addToFilter(input.readLine());
			}
		} catch (IOException ex) {
			log.error("IOException reading from BufferedReader: ", ex);
		}
	}

}
