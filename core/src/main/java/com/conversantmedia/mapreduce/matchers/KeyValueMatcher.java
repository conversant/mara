package com.conversantmedia.mapreduce.matchers;

/**
 * Copyright 2013 Patrick K. Jaromin <patrick@jaromin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static org.hamcrest.Matchers.anything;

import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

/**
 * A {@link org.hamcrest.Matcher} for matching {@link org.apache.hadoop.hbase.KeyValue}
 * entries in a HBase {@link org.apache.hadoop.hbase.client.Put}. This is designed
 * primarily for use in JUnit/MRUnit test cases for validating the outcome of a 
 * Map-Reduce job outputting to HBase.
 * 
 * Examples of usage:
 * 
 *  // For a 'put' operation...
 *	Put put = new Put(rowKey);
 *	put.add(Bytes.toBytes("a"), "column1".getBytes(), "avalue1".getBytes());
 *
 *	// These would pass the assertion
 *	assertThat(put, hasKeyValue(hasColumn("a:column1"), "avalue1"));
 *	assertThat(put, hasKeyValue(hasColumn("a:column1"), is("avalue1")));
 *	assertThat(put, hasKeyValue(hasColumn(is("a:column1")), is("avalue1")));
 *	assertThat(put, hasKeyValue(hasColumn(startsWith("a:col")), containsString("value")));
 * 
 * See more usage examples in the test cases included in this package.
 * in {@link com.conversantmedia.mapreduce.matchers.KeyValueMatcherTest}
 * 
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 * @param <COL>
 * @param <VAL>
 */
public class KeyValueMatcher<COL,VAL> extends TypeSafeDiagnosingMatcher<Mutation> {

	private ColumnMatcher<COL> columnMatcher;
	
	private final Matcher<VAL> valueMatcher;
	
	private final Class<VAL> valueClass;

	public KeyValueMatcher(Matcher<VAL> valueMatcher, Class<VAL> valueClass) {
		this(null, valueMatcher, valueClass);
	}

	@SuppressWarnings("unchecked")
	public KeyValueMatcher(ColumnMatcher<COL> columnMatcher, Matcher<VAL> valueMatcher,
			Class<VAL> valueClass) {
		this.columnMatcher = columnMatcher;
		this.valueMatcher = valueMatcher;
		this.valueClass = valueClass;
		
		if (this.columnMatcher == null) {
			// initialize with one that will match ANY
			this.columnMatcher = new ColumnMatcher<>((Matcher<COL>)anything());
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.hamcrest.TypeSafeDiagnosingMatcher#matchesSafely(java.lang.Object, org.hamcrest.Description)
	 */
	@Override
	protected boolean matchesSafely(Mutation mutation, Description mismatch) {	
		// Delegate check for column match to 
		List<Cell> matchingKeyValues = columnMatcher.findMatches(mutation, mismatch, false);
		if (matchingKeyValues.size() == 0) {
			columnMatcher.describeMismatch(mutation, mismatch);
			return false;
		}

		// Check the key-values for a matching value
		int count = 0;
		for (Cell columnMatch : matchingKeyValues) {
			byte[] valueBytes = CellUtil.cloneValue(columnMatch);
			VAL value = Matchers.valueOf(valueBytes, this.valueClass);
			if (valueMatcher.matches(value)){
				return true;
			}
			if (count++ > 0) {
            	mismatch.appendText(", ");
            }
            valueMatcher.describeMismatch(value, mismatch);
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see org.hamcrest.SelfDescribing#describeTo(org.hamcrest.Description)
	 */
	@Override
	public void describeTo(Description mismatch) {
		mismatch.appendText("a column value matching ");
		valueMatcher.describeTo(mismatch);
	}

}
