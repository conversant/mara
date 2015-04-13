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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import com.conversantmedia.mapreduce.tool.DistributedResourceManager;

/**
 *
 *
 */
public class DistributedResourceManagerTest {

	@Test
	public void testSetFieldValue() {

		Object bean = new TargetBean();


		Map<String, String> fieldVals = new HashMap<String,String>();
		fieldVals.put("b","true");
		fieldVals.put("c","a");
		fieldVals.put("i","475");
		fieldVals.put("s","25");
		fieldVals.put("f","999.99");
		fieldVals.put("d","1234567.0");
		fieldVals.put("l","123456789");

		fieldVals.put("B","false");
		fieldVals.put("C","Z");
		fieldVals.put("I","4750");
		fieldVals.put("S","50");
		fieldVals.put("F","1999.99");
		fieldVals.put("D","1234567.0");
		fieldVals.put("L","1234567890");
		try {
			for (Entry<String,String> e : fieldVals.entrySet()) {
				Field field = bean.getClass().getDeclaredField(e.getKey());
				String value = e.getValue();
				DistributedResourceManager.setFieldValue(field, bean, value);
				assertThat(field.get(bean).toString(), equalTo(value));
			}

		} catch (NoSuchFieldException | SecurityException | IllegalAccessException e) {
			fail(e.getMessage());
		}

//		System.out.println(ToStringBuilder.reflectionToString(bean));
	}


	@SuppressWarnings("unused")
	private static class TargetBean {
		private boolean b;
		private char c;
		private int i;
		private short s;
		private float f;
		private double d;
		private long l;

		private Boolean B;
		private Character C;
		private Integer I;
		private Short S;
		private Float F;
		private Double D;
		private Long L;
	}
}
