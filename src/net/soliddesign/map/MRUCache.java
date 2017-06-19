package net.soliddesign.map;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class MRUCache<T> {
	private List<T> list;

	public MRUCache(int size) {
		list = new ArrayList<>(size);
	}

	public void add(T t) {
		list.set(list.size() - 1, t);
	}

	public T get(Predicate<T> p) {
		T t;
		if (p.test(t = list.get(0))) {
			return t;
		}
		for (int i = 1; i < list.size(); i++) {
			if (p.test(t = list.get(i))) {
				list.set(i, list.get(i - 1));
				list.set(i - 1, t);
				return t;
			}
		}
		return null;
	}
}
