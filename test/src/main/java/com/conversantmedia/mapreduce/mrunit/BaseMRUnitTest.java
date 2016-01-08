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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mrunit.TestDriver;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.mockito.verification.VerificationMode;
import org.springframework.stereotype.Service;

import com.conversantmedia.mapreduce.exception.UnsupportedUsageException;
import com.conversantmedia.mapreduce.io.avro.AvroUtil;
import com.conversantmedia.mapreduce.tool.ToolException;
import com.conversantmedia.mapreduce.tool.annotation.AvroDefault;
import com.conversantmedia.mapreduce.tool.annotation.AvroJobInfo;
import com.conversantmedia.mapreduce.tool.annotation.AvroNamedOutput;
import com.conversantmedia.mapreduce.tool.annotation.DriverContext;
import com.conversantmedia.mapreduce.tool.annotation.JobInfo;
import com.conversantmedia.mapreduce.tool.annotation.MapperInfo;
import com.conversantmedia.mapreduce.tool.annotation.MapperService;
import com.conversantmedia.mapreduce.tool.annotation.NamedOutput;
import com.conversantmedia.mapreduce.tool.annotation.ReducerInfo;
import com.conversantmedia.mapreduce.tool.annotation.ReducerService;
import com.conversantmedia.mapreduce.tool.annotation.ToolContext;
import com.conversantmedia.mapreduce.tool.annotation.handler.AvroJobInfoAnnotationHandler;
import com.conversantmedia.mapreduce.tool.annotation.handler.AvroNamedOutputAnnotationHandler;
import com.conversantmedia.mapreduce.tool.annotation.handler.MaraAnnotationUtil;
import com.conversantmedia.mapreduce.tool.annotation.handler.NamedOutputAnnotationHandler;

/**
 *
 *
 * @param <A> The annotated driver type
 * @param <D> The {@link TestDriver} type
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class BaseMRUnitTest<D extends TestDriver<K2, V2, T>, A,
K1, V1, K2, V2, T extends TestDriver<K2, V2, T>> {

	@Rule public TestName name = new TestName();

	private D testDriver;
	protected A annotatedDriver;
	private Class<A> annotatedDriverClass;

	protected Mapper mapper;
	protected Reducer reducer;

	protected Job job;

	protected Map<String,MultipleOutputs> namedOutputs;

	protected Map<String,AvroMultipleOutputs> avroNamedOutputs;

	protected BaseMRUnitTest() {
		Type superclass = getClass().getGenericSuperclass();
		if (superclass == null) {
			throw new UnsupportedUsageException("Must specify appropriate type parameters in test class.");
		}
		this.annotatedDriverClass = (Class<A>)((ParameterizedType) superclass).getActualTypeArguments()[0];
		this.annotatedDriver = mock(this.annotatedDriverClass);
	}

	public abstract D initializeTestDriver(Mapper mapper, Reducer reducer);

	/**
	 * Override to implement standard Job init.
	 * Base class no-arg implementation.
	 * 
	 * @throws Exception	if something goes wrong.
	 */
	public void setup() throws Exception {
		/** do nothing */
	}

	@Before
	public void initializeJobAndDriver() throws Exception {

		Class<?> driverClass = getDriverClass();

		this.mapper = getMapperFor(driverClass);
		this.reducer = getReducerFor(driverClass);

		// Setup distributed cache
		UnitTestDistributedResourceManager manager = new UnitTestDistributedResourceManager();
		manager.setupDistributedResources(name.getMethodName(),
				driverClass, this, mapper, reducer);
		// Setup for context bean too
		Class<?> contextClass = getContextClass(driverClass);
		if (contextClass != null) {
			manager.setupDistributedResources(name.getMethodName(),
					contextClass, this, mapper, reducer);
		}

		this.testDriver = initializeTestDriver(this.mapper, this.reducer);

		Configuration config = this.testDriver.getConfiguration();
		this.job = mock(Job.class);
		when(job.getConfiguration()).thenReturn(config);

		// AvroJobInfo
		Field avroField = MaraAnnotationUtil.INSTANCE.findAnnotatedField(driverClass, AvroJobInfo.class);
		if (avroField != null) {
			AvroJobInfoAnnotationHandler avroHandler = new AvroJobInfoAnnotationHandler();
			AvroJobInfo avroAnnotation = avroField.getAnnotation(AvroJobInfo.class);
			avroHandler.process(avroAnnotation, job, null);
			// For unit tests we also need to setup the map output reader/writer schema
			// even if this is a reducer test outputting to the default context
			if (avroAnnotation.inputKeySchema() != AvroDefault.class) {
				AvroSerialization.setKeyReaderSchema(job.getConfiguration(),
						AvroUtil.getSchema(avroAnnotation.inputKeySchema()));
			}
			if (avroAnnotation.inputValueSchema() != AvroDefault.class) {
				AvroSerialization.setValueReaderSchema(job.getConfiguration(),
						AvroUtil.getSchema(avroAnnotation.inputValueSchema()));
			}
			configureAvroSchemas(avroAnnotation);
		}

		// Named outputs?
		namedOutputs = new HashMap<>();
		avroNamedOutputs = new HashMap<>();
		if (this.mapper != null) {
			setupMultipleOutputs(this.mapper);
		}
		if (this.reducer != null) {
			setupMultipleOutputs(this.reducer);
		}

		setup();
	}

	protected void configureAvroSchemas(AvroJobInfo avroAnnotation)
			throws Exception {
		if (avroAnnotation.mapOutputKeySchema() != AvroDefault.class) {
			AvroSerialization.setKeyWriterSchema(job.getConfiguration(),
					AvroUtil.getSchema(avroAnnotation.mapOutputKeySchema()));
		}
		if (avroAnnotation.mapOutputValueSchema() != AvroDefault.class) {
			AvroSerialization.setValueWriterSchema(job.getConfiguration(),
					AvroUtil.getSchema(avroAnnotation.mapOutputValueSchema()));
		}

		if (avroAnnotation.outputKeySchema() != AvroDefault.class) {
			AvroSerialization.setKeyWriterSchema(job.getConfiguration(),
					AvroUtil.getSchema(avroAnnotation.outputKeySchema()));
		}
		if (avroAnnotation.outputValueSchema() != AvroDefault.class) {
			AvroSerialization.setValueWriterSchema(job.getConfiguration(),
					AvroUtil.getSchema(avroAnnotation.outputValueSchema()));
		}
	}

	public D getTestDriver() {
		return this.testDriver;
	}

	/**
	 * The class of the driver under test.
	 * 
	 * @return Class the driver class under test
	 */
	protected Class<A> getDriverClass() {
		return this.annotatedDriverClass;
	}

	protected Class<?> getContextClass(Class<?> driverClass) {
		Field field = MaraAnnotationUtil.INSTANCE.findAnnotatedField(driverClass, DriverContext.class, ToolContext.class);
		if (field != null) {
			return (Class)field.getType();
		}
		return null;
	}

	protected void setupMultipleOutputs(Object component) throws IllegalArgumentException, IllegalAccessException, ToolException {
		Class componentClass = component.getClass();
		if (MaraAnnotationUtil.hasAnyAnnotation(componentClass,
				Service.class, MapperService.class, ReducerService.class)) {
			NamedOutputAnnotationHandler handler = new NamedOutputAnnotationHandler();
			List<Field> fields = MaraAnnotationUtil.INSTANCE.findAnnotatedFields(componentClass, NamedOutput.class);
			for (Field field : fields) {
				String[] names = handler.getNames(field.getAnnotation(NamedOutput.class));
				for (String name : names) {
					if (!this.namedOutputs.containsKey(name)) {
						MultipleOutputs namedOutput = mock(MultipleOutputs.class);
						this.namedOutputs.put(name, namedOutput);
						field.setAccessible(true);
						field.set(component, namedOutput);
					}
				}
			}

			AvroNamedOutputAnnotationHandler avroHandler = new AvroNamedOutputAnnotationHandler();
			fields = MaraAnnotationUtil.INSTANCE.findAnnotatedFields(componentClass, AvroNamedOutput.class);
			for (Field field : fields) {
				AvroNamedOutput avroNamedOutputAnnotation = field.getAnnotation(AvroNamedOutput.class);
				String[] names = avroHandler.getNames(avroNamedOutputAnnotation);
				Schema schema = avroHandler.getSchema(avroNamedOutputAnnotation.record());
				for (String name : names) {
					if (!this.avroNamedOutputs.containsKey(name)) {
						AvroMultipleOutputs avroNamedOutput = mock(AvroMultipleOutputs.class);
						this.avroNamedOutputs.put(name, avroNamedOutput);
						field.setAccessible(true);
						field.set(component, avroNamedOutput);

						AvroMultipleOutputs.addNamedOutput(job, name, avroNamedOutputAnnotation.format(), schema);
					}
				}
			}
		}
	}

	protected Mapper getMapperFor(Class<?> driverClass)
			throws InstantiationException, IllegalAccessException {
		Mapper mapper = null;
		Field jobField = MaraAnnotationUtil.INSTANCE.findAnnotatedField(driverClass, JobInfo.class);
		MapperInfo info = jobField.getAnnotation(MapperInfo.class);
		if (info != null && info.value() != null) {
			Class<? extends Mapper> mapperClass = info.value();
			mapper = mapperClass.newInstance();
		}
		return mapper;
	}

	protected Reducer getReducerFor(Class<?> driverClass)
			throws InstantiationException, IllegalAccessException {
		Reducer reducer = null;
		Field jobField = MaraAnnotationUtil.INSTANCE.findAnnotatedField(driverClass, JobInfo.class);
		ReducerInfo info = jobField.getAnnotation(ReducerInfo.class);
		if (info != null && info.value() != null) {
			Class<? extends Reducer> reducerClass = info.value();
			reducer = reducerClass.newInstance();
		}
		return reducer;
	}

	/**
	 * Returns the first named output found. This is designed as a
	 * convenience method for the typical case where there's only a single
	 * named output. If there are multiple named outputs, the one returned is essentially random
	 * and the caller should therefore use the {@link #getNamedOutput(String name)} version.
	 * 
	 * @return MultipleOutputs the configured named output
	 */
	protected MultipleOutputs getNamedOutput() {
		if (this.namedOutputs.size() > 0) {
			return this.namedOutputs.entrySet().iterator().next().getValue();
		}
		return null;
	}

	protected MultipleOutputs getDefaultNamedOutput() {
		return getNamedOutput("default");
	}

	protected MultipleOutputs getNamedOutput(String name) {
		return this.namedOutputs.get(name);
	}

	/**
	 * Returns the first avro named output found. This is designed as a
	 * convenience method for the typical case where there's only a single
	 * avro named output. If there are multiple named outputs, the one returned is essentially random
	 * and the caller should therefore use the {@link #getNamedOutput(String name)} version.
	 * 
	 * @return AvroMultipleOutputs the configured Avro multiple output
	 */
	protected AvroMultipleOutputs getAvroNamedOutput() {
		if (this.avroNamedOutputs.size() > 0) {
			return this.avroNamedOutputs.entrySet().iterator().next().getValue();
		}
		return null;
	}

	protected AvroMultipleOutputs getDefaultAvroNamedOutput() {
		return getAvroNamedOutput("default");
	}

	protected AvroMultipleOutputs getAvroNamedOutput(String name) {
		return this.avroNamedOutputs.get(name);
	}

	/**
	 * Check using the write(name, key, value) method, 1x
	 * 
	 * @param name	the named output name
	 * @param key	the key to verify was written
	 * @param value	the value to verify was written
	 */
	protected void verifyNamedOutput(String name, Object key, Object value) {
		verifyNamedOutput(getNamedOutput(name), name, key, value, null, times(1));
	}

	/**
	 * Check using the write(name, key, value) method
	 * 
	 * @param name	the named output name
	 * @param key	the key to verify was written
	 * @param value	the value to verify was written
	 * @param mode	the verification - commonly times() or never()
	 */
	protected void verifyNamedOutput(String name, Object key, Object value, VerificationMode mode) {
		verifyNamedOutput(getNamedOutput(name), name, key, value, null, mode);
	}

	/**
	 * Verify calls to write(key, value, basePath)
	 * 
	 * @param key	the key to verify was written
	 * @param value	the value to verify was written
	 * @param path	path the output should have written to
	 * @param mode	the verification - commonly times() or never()
	 */
	protected void verifyNamedOutput(Object key, Object value, String path, VerificationMode mode) {
		verifyNamedOutput(getNamedOutput(), null, key, value, path, mode);
	}

	/**
	 * Verify calls to write(key, value, basePath), 1x.
	 * 
	 * @param key	the key to verify was written
	 * @param value	the value to verify was written
	 * @param path	path the output should have written to
	 */
	protected void verifyNamedOutput(Object key, Object value, String path) {
		verifyNamedOutput(getNamedOutput(), null, key, value, path, times(1));
	}

	protected void verifyNamedOutput(String name, Object key, Object value, String path) {
		verifyNamedOutput(getNamedOutput(name), name, key, value, path, times(1));
	}

	protected void verifyNamedOutput(String name, Object key, Object value, String path, int times) {
		verifyNamedOutput(getNamedOutput(name), name, key, value, path, times(times));
	}

	protected void verifyNamedOutput(String name, Object key, Object value, String path, VerificationMode mode) {
		verifyNamedOutput(getNamedOutput(name), name, key, value, path, mode);
	}

	protected void verifyNamedOutput(MultipleOutputs multiOut, String name, Object key, Object value, String path, VerificationMode mode) {
		ArgumentCaptor keyArg = ArgumentCaptor.forClass(key.getClass());
		ArgumentCaptor valueArg = ArgumentCaptor.forClass(value.getClass());
		try {
			if (name == null) {
				verify(multiOut, mode).write(keyArg.capture(), valueArg.capture(), path);
			}
			else {
				if (path == null) {
					verify(multiOut, mode).write(eq(name), keyArg.capture(), valueArg.capture());
					assertEquals(key, keyArg.getValue());
					assertEquals(value, valueArg.getValue());
				}
				else {
					verify(multiOut, mode).write(name, keyArg.capture(), valueArg.capture(), path);
				}
			}
		} catch (IOException | InterruptedException e) {
			fail(e.getMessage());
		}
	}

	protected void verifyNamedOutput(MultipleOutputs multiOut, String name, Object key, Object value, VerificationMode mode) {
		ArgumentCaptor keyArg = ArgumentCaptor.forClass(key.getClass());
		ArgumentCaptor valueArg = ArgumentCaptor.forClass(value.getClass());
		try {
			verify(multiOut, mode).write(name, keyArg.capture(), valueArg.capture());
		} catch (IOException | InterruptedException e) {
			fail(e.getMessage());
		}
	}
}
