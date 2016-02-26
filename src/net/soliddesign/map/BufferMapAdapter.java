package net.soliddesign.map;

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
	private Map<ByteBuffer, ByteBuffer> map;

	public BufferMapAdapter(Map<ByteBuffer, ByteBuffer> map, Function<K, ByteBuffer> fromKey,
			Function<ByteBuffer, K> toKey, Function<V, ByteBuffer> fromValue, Function<ByteBuffer, V> toValue) {
		this.map = map;
		this.toKey = toKey;
		this.fromKey = fromKey;
		this.toValue = toValue;
		this.fromValue = fromValue;
	}

	@Override
	public void clear() {
		map.clear();
	}

	@Override
	public void close() throws Exception {
		if (map instanceof AutoCloseable) {
			((AutoCloseable) map).close();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean containsKey(Object key) {
		return map.containsKey(fromKey((K) key));
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean containsValue(Object value) {
		return map.containsValue(fromValue((V) value));
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return map.entrySet().stream()
				.map(entry -> new AbstractMap.SimpleEntry<K, V>(toKey(entry.getKey()), toValue(entry.getValue())))
				.collect(Collectors.toSet());
	}

	@Override
	public boolean equals(Object obj) {
		return entrySet().equals(obj);
	}

	private ByteBuffer fromKey(K key) {
		ByteBuffer bb = fromKey.apply(key);
		bb.rewind();
		return bb;
	}

	private ByteBuffer fromValue(V value) {
		ByteBuffer bb = fromValue.apply(value);
		bb.rewind();
		return bb;
	}

	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		return toValue(map.get(fromKey((K) key)));
	}

	@Override
	public int hashCode() {
		return entrySet().hashCode();
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public Set<K> keySet() {
		return map.keySet().stream().map(keyBuffer -> toKey(keyBuffer)).collect(Collectors.toSet());
	}

	@Override
	public V put(K key, V value) {
		ByteBuffer oldBuffer = map.put(fromKey(key), fromValue(value));
		return oldBuffer == null ? null : toValue(oldBuffer);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> otherMap) {
		otherMap.entrySet().stream().forEach(entry -> map.put(fromKey(entry.getKey()), fromValue(entry.getValue())));
	}

	@Override
	public V remove(Object key) {
		@SuppressWarnings("unchecked")
		ByteBuffer oldBuffer = map.remove(fromKey((K) key));
		return oldBuffer == null ? null : toValue(oldBuffer);
	}

	@Override
	public int size() {
		return map.size();
	}

	private K toKey(ByteBuffer bb) {
		K key = toKey.apply(bb);
		bb.rewind();
		return key;
	}

	@Override
	public String toString() {
		return entrySet().toString();
	}

	private V toValue(ByteBuffer bb) {
		V value = null;
		if (bb != null) {
			value = toValue.apply(bb);
			bb.rewind();
		}
		return value;
	}

	@Override
	public Collection<V> values() {
		return map.values().stream().map(valueBuffer -> toValue(valueBuffer)).collect(Collectors.toSet());
	}
}