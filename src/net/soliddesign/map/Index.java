package net.soliddesign.map;

import java.util.Map;
import java.util.function.Function;

public class Index<T, K> {
	final private Function<T, K> fn;
	final private Map<K, T> map;

	public Index(Map<K, T> map, Function<T, K> fn) {
		this.map = map;
		this.fn = fn;
	}

	void insert(T obj) {
		map.put(fn.apply(obj), obj);
	}

}
