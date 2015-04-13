package com.conversantmedia.mapreduce.tool.annotation.handler;

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


import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.annotation.Annotation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.conversantmedia.mapreduce.tool.AnnotatedTool;
import com.conversantmedia.mapreduce.tool.AnnotatedToolContext;
import com.conversantmedia.mapreduce.tool.DriverContextBase;
import com.conversantmedia.mapreduce.tool.ToolException;
import com.conversantmedia.mapreduce.tool.annotation.TableInput;
import com.conversantmedia.mapreduce.tool.annotation.handler.TableInputAnnotationHandler;

// FIXME Tests hang since upgrade of hbase/mr
@Ignore
public class TableInputAnnotationHandlerTest {

	private static final String TEST_INPUT = "INPUT_TABLE";

	TableInputAnnotationHandler handler;

	AnnotatedTool tool;
	DriverContextBase context;
	Job job;
	Configuration conf;

	@Test
	public void testProcessDefaults() {

		try {
			Annotation annotation = setupDriver(new TableDriverDefaults());

			handler.process(annotation, job, null);

			verify(job, times(1)).setInputFormatClass(TableInputFormat.class);
			assertThat(conf.get(TableInputFormat.INPUT_TABLE), equalTo(TEST_INPUT));
			assertThat(conf.get(TableInputFormat.SCAN), equalTo("AgAAAAAAAf//////////AQAAAAAAAAAAAH//////////AQAAAAAAAAAA"));

		} catch (ToolException | NoSuchFieldException | SecurityException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testProcessExplicitTable() {

		try {
			Annotation annotation = setupDriver(new TableDriverExplicitTable());
			handler.process(annotation, job, null);

			verify(job, times(1)).setInputFormatClass(TableInputFormat.class);
			assertThat(conf.get(TableInputFormat.INPUT_TABLE), equalTo("my_table"));
			assertThat(conf.get(TableInputFormat.SCAN), equalTo("AgAAAAAAAf//////////AQAAAAAAAAAAAH//////////AQAAAAAAAAAA"));

		} catch (ToolException | NoSuchFieldException | SecurityException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testProcessCustomScan() {

		try {
			Annotation annotation = setupDriver(new TableDriverWithScan());

			handler.process(annotation, job, null);

			verify(job, times(1)).setInputFormatClass(TableInputFormat.class);
			assertThat(conf.get(TableInputFormat.INPUT_TABLE), equalTo("test.my_table"));
			assertThat(conf.get(TableInputFormat.SCAN), equalTo("AgAAAAAAAv//////////AQAAAAAAAAAAAH//////////AQAAAAAAAAAA"));

		} catch (ToolException | NoSuchFieldException | SecurityException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testConditionalName() {
		try {
			Annotation annotation = setupDriver(new TableDriverNameExpr());

			handler.process(annotation, job, null);

			verify(job, times(1)).setInputFormatClass(TableInputFormat.class);
			assertThat(conf.get(TableInputFormat.INPUT_TABLE), equalTo("myTable"));

			TableDriverNameExpr.PREFIX = "test";
			handler.process(annotation, job, null);
			assertThat(conf.get(TableInputFormat.INPUT_TABLE), equalTo("test.myTable"));

		} catch (ToolException | NoSuchFieldException | SecurityException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private static final class TableDriverDefaults {
		@TableInput
		Job job;
	}

	private static final class TableDriverExplicitTable {
		@TableInput(table="my_table")
		Job job;
	}

	private static final class TableDriverWithScan {
		@TableInput(table="${prefix + '.my_table'}")
		Job job;

		@SuppressWarnings("unused")
		public Scan getScan() {
			return new Scan().setMaxVersions(2);
		}

		@SuppressWarnings("unused")
		public String getPrefix() {
			return"test";
		}
	}

	private static final class TableDriverNameExpr {
		@TableInput(table="${this.prefix != null? this.prefix + '.' + this.table : this.table}")
		Job job;

		public static String PREFIX = null;

		@SuppressWarnings("unused")
		public String getTable() {
			return "myTable";
		}

		@SuppressWarnings("unused")
		public String getPrefix() {
			return PREFIX;
		}
	}

	@Before
	public void setup() {

		this.conf = new Configuration();
		this.job = mock(Job.class);
		this.tool = mock(AnnotatedTool.class);
		this.context = mock(DriverContextBase.class);

		AnnotatedToolContext annotatedContext = new AnnotatedToolContext(this.context);

		when(job.getConfiguration()).thenReturn(conf);
		when(tool.getContext()).thenReturn(annotatedContext);
		when(this.context.getInput()).thenReturn(TEST_INPUT);

		this.handler = new TableInputAnnotationHandler();
		this.handler.initialize(tool);
	}

	public Annotation setupDriver(Object driver) throws NoSuchFieldException, SecurityException {
		when(tool.getToolBean()).thenReturn(driver);
		return driver.getClass().getDeclaredField("job").getAnnotation(TableInput.class);

	}

	@After
	public void cleanup() {
		this.job = null;
		this.handler= null;
		this.tool = null;
	}
}
