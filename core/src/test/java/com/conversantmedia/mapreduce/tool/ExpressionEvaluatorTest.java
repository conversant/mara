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


import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import com.conversantmedia.mapreduce.tool.ExpressionEvaluator;
import com.conversantmedia.mapreduce.tool.ToolException;

public class ExpressionEvaluatorTest {

	@Test @SuppressWarnings({"unchecked","rawtypes"})
	public void testEvaluateExpression() {

		Object rootObj = new ToolBean();
		ContextBean ctxBean = new ContextBean();
		Map contextMap = new HashMap();
		contextMap.put("context", ctxBean);

		Map<String, Object> expressions = new HashMap<String, Object>();
		expressions.put("${name}", ((ToolBean)rootObj).name);
		// not yet working
//		expressions.put("${context.privateMember}", ctxBean.privateMember);
		expressions.put("The name is ${this.name}", "The name is " + ((ToolBean)rootObj).name); // may use 'this' keyword as well.
		expressions.put("${context.hello} there, Frank!",  ctxBean.hello + " there, Frank!");
		expressions.put("If I give you $${context.value1}, will you be my friend?",
				"If I give you $" + ctxBean.value1 + ", will you be my friend?");
		expressions.put("I wouldn't be your friend for $${context.value2}!",
				"I wouldn't be your friend for $" + ctxBean.value2 + "!");

		expressions.put("${context.hello} there, Frank! Please give me $${context.value1} today or ${context.value2} tomorrow.",
				ctxBean.hello + " there, Frank! Please give me $" + ctxBean.value1 + " today or " + ctxBean.value2 + " tomorrow.");

		for (Entry<String,Object> e : expressions.entrySet()) {
			try {
				Object result = ExpressionEvaluator.instance().evaluate(rootObj, contextMap, e.getKey());
				assertThat(result.toString(), equalTo(e.getValue().toString()));
			} catch (ToolException e1) {
				e1.printStackTrace();
				fail(e1.getMessage());
			}

		}
	}

	protected static final class ToolBean {
		public String name = "aTool";
		@SuppressWarnings("unused")
		private String notInvoked = "Not Invoked";

		public String getNotInvoked() throws IllegalAccessException {
			fail("Should not be called.");
			return null;
		}
	}

	protected static final class ContextBean {
		private String hello = "Hello";
		private Integer value1 = Integer.MAX_VALUE;
		private Long value2 = Long.MAX_VALUE;

		// To test private member access.
		@SuppressWarnings("unused")
		private String privateMember = "I'M PRIVATE";

		public String getHello() {
			return hello;
		}
		public void setHello(String hello) {
			this.hello = hello;
		}
		public Integer getValue1() {
			return value1;
		}
		public void setValue1(Integer value1) {
			this.value1 = value1;
		}
		public Long getValue2() {
			return value2;
		}
		public void setValue2(Long value2) {
			this.value2 = value2;
		}
	}

}
