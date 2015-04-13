package com.conversantmedia.mapreduce.tool;

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

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.conversantmedia.mapreduce.tool.AnnotatedDelegatingComponent;

/**
 * Extensible base class for reducers with annotations.
 * Mostly for testing until we get AOP working properly.
 *
 * @param <K1>	input key type
 * @param <V1>	input value type
 * @param <K2>	output key type
 * @param <V2>	output value type
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public final class AnnotatedDelegatingReducer<K1, V1, K2, V2> extends Reducer<K1,V1,K2,V2>
	implements AnnotatedDelegatingComponent<Reducer<K1,V1,K2,V2>> {

	public static final String CONFKEY_DELEGATE_REDUCER_CLASS = AnnotatedDelegatingReducer.class.getName() + ";delegate";

	private Reducer<K1,V1,K2,V2> reducer;

	@Override
	public void run(Reducer<K1,V1,K2,V2>.Context context)
			throws IOException, InterruptedException {
		setup(context);
		getDelegate(context).run(context);
		cleanup(context);
	}

	@Override
	public Reducer<K1,V1,K2,V2> getDelegate(TaskAttemptContext context) {
		if (this.reducer == null) {
			Class<? extends Reducer> reducerClass = (Class<? extends Reducer>)context.getConfiguration()
					.getClass(CONFKEY_DELEGATE_REDUCER_CLASS, Reducer.class);
			reducer = (Reducer<K1, V1, K2, V2>) ReflectionUtils.newInstance(
					reducerClass, context.getConfiguration());
		}
		return this.reducer;
	}

}
