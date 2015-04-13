package com.conversantmedia.mapreduce.mrunit;

/*
 * #%L
 * Mara Unit Test Framework
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


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

@SuppressWarnings({"unchecked","rawtypes"})
public class ReduceDriverTestBase<A, K1, V1, K2, V2>
	extends BaseMRUnitTest<ReduceDriver<K1, V1, K2, V2>, A, K1, V1, K2, V2, ReduceDriver<K1, V1, K2, V2>> {

	@Override
	public ReduceDriver initializeTestDriver(Mapper mapper, Reducer reducer) {
		return ReduceDriver.newReduceDriver(reducer);
	}
}
