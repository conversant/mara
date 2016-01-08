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

import static org.hamcrest.CoreMatchers.is;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Matcher;

/**
 * Convenience class for enabling simpler importing of 
 * the suite of matchers:
 * <tt>import static com.jaromin.hbase.matchers.Matchers.*;</tt>
 * 
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 */
public abstract class Matchers {

	/**
	 * 
	 * @param <T>
	 * @param bytes
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T valueOf(byte[] bytes, Class<? extends T> valueClass) {
		if (byte[].class.equals(valueClass)) {
			return (T)bytes;
		} else if (String.class.equals(valueClass)) {
			return (T)Bytes.toString(bytes);
		}
		else if (Long.class.equals(valueClass)) {
			return (T)Long.valueOf(Bytes.toLong(bytes));
		}
		else if (Double.class.equals(valueClass)) {
			return (T)Double.valueOf(Bytes.toDouble(bytes));
		}
		else if (Float.class.equals(valueClass)) {
			return (T)Float.valueOf(Bytes.toFloat(bytes));
		}
		else if (Integer.class.equals(valueClass)) {
			return (T)Integer.valueOf(Bytes.toInt(bytes));
		}
		else if (Short.class.equals(valueClass)) {
			return (T)Short.valueOf(Bytes.toShort(bytes));
		}
		return null;
	}

	public static <T> RowKeyMatcher<T> hasRowKey(Matcher<T> expected, Class<T> typeClass) {
		return new RowKeyMatcher<>(expected, typeClass);
	}

	public static RowKeyMatcher<byte[]> hasRowKey(byte[] expected) {
		return hasRowKey( is(expected), byte[].class);
	}
	
	public static RowKeyMatcher<String> hasRowKey(String expected) {
		return hasRowKey( is(expected), String.class);
	}
	
	public static RowKeyMatcher<String> hasRowKey(Matcher<String> expected) {
		return hasRowKey( is(expected), String.class);
	}
	
	public static RowKeyMatcher<Long> hasRowKey(Long expected) {
		return hasRowKey( is(expected), Long.class);
	}
	
	public static RowKeyMatcher<Double> hasRowKey(Double expected) {
		return hasRowKey( is(expected), Double.class);
	}
	
	public static RowKeyMatcher<Float> hasRowKey(Float expected) {
		return hasRowKey( is(expected), Float.class);
	}

	public static RowKeyMatcher<Integer> hasRowKey(Integer expected) {
		return hasRowKey( is(expected), Integer.class);
	}
	
	public static RowKeyMatcher<Short> hasRowKey(Short expected) {
		return hasRowKey( is(expected), Short.class);
	}
	
	public static ColumnMatcher<String> hasColumn(Matcher<String> matcher) {
		return new ColumnMatcher<>(matcher);
	}

	public static ColumnMatcher<String> hasColumn(String string) {
		return hasColumn(is(string));
	}

	public static ColumnMatcher<Long> hasColumnLong(Matcher<Long> matcher) {
		return new ColumnMatcher<>(matcher);
	}

	public static ColumnMatcher<Long> hasColumnLong(Long value) {
		return hasColumnLong(is(value));
	}

	public static ColumnMatcher<Double> hasColumnDouble(Matcher<Double> matcher) {
		return new ColumnMatcher<>(matcher);
	}

	public static ColumnMatcher<Double> hasColumnDouble(Double Double) {
		return hasColumnDouble(is(Double));
	}
	
	public static ColumnMatcher<Float> hasColumnFloat(Matcher<Float> matcher) {
		return new ColumnMatcher<>(matcher);
	}

	public static ColumnMatcher<Float> hasColumnFloat(Float Float) {
		return hasColumnFloat(is(Float));
	}
	
	public static ColumnMatcher<Integer> hasColumnInteger(Matcher<Integer> matcher) {
		return new ColumnMatcher<>(matcher);
	}

	public static ColumnMatcher<Integer> hasColumnInteger(Integer Integer) {
		return hasColumnInteger(is(Integer));
	}
	
	public static ColumnMatcher<Short> hasColumnShort(Matcher<Short> matcher) {
		return new ColumnMatcher<>(matcher);
	}

	public static ColumnMatcher<Short> hasColumnShort(Short value) {
		return hasColumnShort(is(value));
	}
	
	public static ColumnMatcher<byte[]> hasColumnBytes(Matcher<byte[]> matcher) {
		return new ColumnMatcher<>(matcher);
	}

	public static ColumnMatcher<byte[]> hasColumnBytes(byte[] bytes) {
		return hasColumnBytes(is(bytes));
	}
	
	public static Matcher<Mutation> hasKeyValue(Matcher<String> valueMatcher) {
		return new KeyValueMatcher<String, String>(valueMatcher,String.class);
	}

	public static Matcher<Mutation> hasKeyValue(ColumnMatcher<String> columnMatcher, String value) {
		return new KeyValueMatcher<>(columnMatcher,is(value),String.class);
	}
	
	public static Matcher<Mutation> hasKeyValue(ColumnMatcher<String> columnMatcher, Matcher<String> valueMatcher) {
		return new KeyValueMatcher<>(columnMatcher,valueMatcher,String.class);
	}

	public static Matcher<Mutation> hasLongKeyValue(ColumnMatcher<String> columnMatcher, Matcher<Long> valueMatcher) {
		return new KeyValueMatcher<>(columnMatcher,valueMatcher,Long.class);
	}

	public static Matcher<Mutation> hasDoubleKeyValue(ColumnMatcher<String> columnMatcher, Matcher<Double> valueMatcher) {
		return new KeyValueMatcher<>(columnMatcher,valueMatcher,Double.class);
	}
	
	public static Matcher<Mutation> hasIntegerKeyValue(ColumnMatcher<String> columnMatcher, Matcher<Integer> valueMatcher) {
		return new KeyValueMatcher<>(columnMatcher,valueMatcher,Integer.class);
	}

	public static Matcher<Mutation> hasFloatKeyValue(ColumnMatcher<String> columnMatcher, Matcher<Float> valueMatcher) {
		return new KeyValueMatcher<>(columnMatcher,valueMatcher,Float.class);
	}

	public static Matcher<Mutation> hasShortKeyValue(ColumnMatcher<String> columnMatcher, Matcher<Short> valueMatcher) {
		return new KeyValueMatcher<>(columnMatcher,valueMatcher,Short.class);
	}

	public static Matcher<Mutation> hasBytesKeyValue(ColumnMatcher<String> columnMatcher, Matcher<byte[]> valueMatcher) {
		return new KeyValueMatcher<>(columnMatcher,valueMatcher,byte[].class);
	}

}
