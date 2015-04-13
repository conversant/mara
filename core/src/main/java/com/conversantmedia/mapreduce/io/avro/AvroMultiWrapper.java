package com.conversantmedia.mapreduce.io.avro;

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


/**
 * A wrapper for Avro serialization that isn't limited
 * to single key and single value schema.
 *
 * @param <T> Avro record type
 */
public class AvroMultiWrapper<T> {

	private T datum;

	public AvroMultiWrapper() {
	}

	public AvroMultiWrapper(T datum) {
		this.datum = datum;
	}

	public void datum(T datum) {
		this.datum = datum;
	}

	public T datum() {
		return this.datum;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (datum == null ? 0 : datum.hashCode());
		return result;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		AvroMultiWrapper other = (AvroMultiWrapper) obj;
		if (datum == null) {
			if (other.datum != null) {
				return false;
			}
		} else if (!datum.equals(other.datum)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return datum.toString();
	}
}
