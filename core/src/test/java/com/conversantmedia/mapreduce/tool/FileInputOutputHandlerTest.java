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


import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.fail;

public class FileInputOutputHandlerTest {

	@Test
	public void testParseContext() {
		MyBeanA a = new MyBeanA();
		a.stringA = "IN A";
		a.p = new Path("/a/path");

		MyBeanB b = new MyBeanB();
		b.stringB = "IN B";
		b.p = new Path("/b/path");

		try {

			@SuppressWarnings("unchecked")
			Map<String, Object> ctx = PropertyUtils.describe(a);
			ctx.put("context", PropertyUtils.describe(b));

			String value = BeanUtils.getProperty(ctx, "context.stringB");
			System.out.println("VALUE = " + value);

			Object val = PropertyUtils.getNestedProperty(ctx, "stringA");
			System.out.println("Value object is: " + val);

			Object path = PropertyUtils.getNestedProperty(ctx, "path");
			if (path instanceof Path) {
				System.out.println("Got a path: " + path);
			}

			path = PropertyUtils.getNestedProperty(ctx, "context.path");
			if (path instanceof Path) {
				System.out.println("Got a path: " + path);
			}


		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public static class MyBeanA {
		public String stringA;
		public Path p;
		public String getStringA() { return this.stringA; }
		public Path getPath() { return p; }
	}

	public static class MyBeanB {
		public String stringB;
		public Long x;
		public Path p;
		public String getStringB() { return this.stringB; }
		public Long getX() { return this.x; }
		public Path getPath() { return p; }
	}

}
