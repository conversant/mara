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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

/**
 * A {@link org.hamcrest.Matcher} for matching 
 * {@link import org.apache.hadoop.hbase.client.Mutation} item column 
 * names. This is designed primarily for use in JUnit/MRUnit test cases 
 * for validating the outcome of a Map-Reduce job outputting to HBase.
 * 
 * The "column name" in this context actually means the string representation
 * of the column name as expressed by the column-family name, a colon,
 * and the column qualifier. This enables users to match against the full
 * column using a single string matcher - including partial matches such as
 * startsWith, endsWith, or containsString. However, because we only deal 
 * with string representations, if your column qualifier is other than 
 * a string, you will not be able to use this matcher.
 * 
 * Examples of usage:
 * 
 *  // For a 'put' operation...
 *	Put put = new Put(rowKey);
 *	put.add(Bytes.toBytes("a"), "column1".getBytes(), "avalue1".getBytes());
 *
 *	// These would pass the assertion
 *	assertThat(put, hasColumn("a:column1"));
 *	assertThat(put, hasColumn(is("a:column1")));
 *	assertThat(put, hasColumn(startsWith("a:col")));
 *	assertThat(put, hasColumn(not(startsWith("b:col"))));
 *  assertThat(put, hasColumn(containsString("value")));
 *
 * See more usage examples in the test cases included in this package.
 * in {@link com.conversantmedia.mapreduce.matchers.ColumnMatcherTest}
 *
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 * @param <T>
 */
public class ColumnMatcher<T> extends TypeSafeDiagnosingMatcher<Mutation> {

	private final Matcher<? super T> nameMatcher;
	
	/**
	 *
	 * @param nameMatcher
	 */
	public ColumnMatcher(Matcher<T> nameMatcher) {
		this.nameMatcher = nameMatcher;
	}

	/*
	 * (non-Javadoc)
	 * @see org.hamcrest.TypeSafeDiagnosingMatcher#matchesSafely(java.lang.Object, org.hamcrest.Description)
	 */
	@Override
	protected boolean matchesSafely(Mutation mutation, Description mismatch) {
		return findMatches(mutation, mismatch, true).size() > 0;
	}

	/**
	 * 
	 * @param mutation change
	 * @param mismatch mismatch
	 * @param stopOnFirstMatch
	 * @return
	 */
	protected List<Cell> findMatches(Mutation mutation, Description mismatch, boolean stopOnFirstMatch) {
		List<Cell> matches = new ArrayList<>();
		Map<byte[], List<Cell>> familyMap = mutation.getFamilyCellMap();
		int count = 0;
		String columnName;
		for (Entry<byte[], List<Cell>> family : familyMap.entrySet()) {
			// Family must be composed of printable characters
			String familyStr = Bytes.toString(family.getKey());
			for (Cell column : family.getValue()) {
				String qualifier = Bytes.toString(CellUtil.cloneQualifier(column));
				// Match the name using the supplied matcher.
				columnName = familyStr + ":" + qualifier;
				if (this.nameMatcher.matches(columnName)) {
					matches.add(column);
					if (stopOnFirstMatch) {
						return matches;
					}
				}
				if (count++ > 0) {
					mismatch.appendText(", ");
				}
				nameMatcher.describeMismatch(columnName, mismatch);
			}
		}
		return matches;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.hamcrest.SelfDescribing#describeTo(org.hamcrest.Description)
	 */
	@Override
	public void describeTo(Description mismatch) {
		mismatch.appendText("a column name matching ");
		nameMatcher.describeTo(mismatch);
	}

}
