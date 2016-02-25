package net.soliddesign.map;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BufferMapAdapter<K, V> implements CloseableMap<K, V> {
	private Function<ByteBuffer, K> toKey;
	private Function<K, ByteBuffer> fromKey;
	private Function<ByteBuffer, V> toValue;
	private Function<V, ByteBuffer> fromValue;

	public BufferMapAdapter(Map<ByteBuffer, ByteBuffer> map, Function<K, ByteBuffer> fromKey,
			Function<ByteBuffer, K> toKey, Function<V, ByteBuffer> fromValue, Function<ByteBuffer, V> toValue) {
		this.map = map;
		this.toKey = toKey;
		this.fromKey = fromKey;
		this.toValue = toValue;
		this.fromValue = fromValue;
	}

	private Map<ByteBuffer, ByteBuffer> map;

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean containsKey(Object key) {
		return map.containsKey(fromKey.apply((K) key));
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean containsValue(Object value) {
		return map.containsValue(fromValue.apply((V) value));
	}

	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		return toValue.apply(map.get(fromKey.apply((K) key)));
	}

	@Override
	public V put(K key, V value) {
		ByteBuffer o = map.put(fromKey.apply(key), fromValue.apply(value));
		return o == null ? null : toValue.apply(o);
	}

	@Override
	public V remove(Object key) {
		@SuppressWarnings("unchecked")
		ByteBuffer o = map.remove(fromKey.apply((K) key));
		return o == null ? null : toValue.apply(o);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		m.entrySet().stream().forEach(e -> map.put(fromKey.apply(e.getKey()), fromValue.apply(e.getValue())));
	}

	@Override
	public void clear() {
		map.clear();
	}

	@Override
	public Set<K> keySet() {
		return map.keySet().stream().map(x -> toKey.apply(x)).collect(Collectors.toSet());
	}

	@Override
	public Collection<V> values() {
		return map.values().stream().map(x -> toValue.apply(x)).collect(Collectors.toSet());
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return map.entrySet().stream()
				.map(m -> new AbstractMap.SimpleEntry<K, V>(toKey.apply(m.getKey()), toValue.apply(m.getValue())))
				.collect(Collectors.toSet());
	}

	@Override
	public int hashCode() {
		return entrySet().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return entrySet().equals(obj);
	}

	@Override
	public void close() throws Exception {
		if (map instanceof Closeable)
			((Closeable) map).close();
	}
}