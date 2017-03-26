package net.soliddesign.map;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BufferMapAdapter<K, V> implements CloseableMap<K, V> {

	private BBBroker<K> keyBroker;
	private BBBroker<V> valueBroker;
	private Map<ByteBuffer, ByteBuffer> map;

	public BufferMapAdapter(Map<ByteBuffer, ByteBuffer> map, String name, BBBroker<K> keyBroker,
			BBBroker<V> valueBroker) {
		this.map = map;
		this.keyBroker = new MagicBroker<>(name, keyBroker);
		this.valueBroker = valueBroker;
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
		return map.containsKey(keyBroker.toBB((K) key));
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean containsValue(Object value) {
		return map.containsValue(valueBroker.toBB((V) value));
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return entryStream().collect(Collectors.toSet());
	}

	private Stream<java.util.Map.Entry<K, V>> entryStream() {
		return map.entrySet().stream()
				.map(entry -> {
					Optional<K> key = rewind(entry.getKey(), keyBroker.fromBB(entry.getKey()));
					if (key.isPresent()) {
						Optional<V> value = rewind(entry.getValue(), valueBroker.fromBB(entry.getValue()));
						if (value.isPresent()) {
							return new AbstractMap.SimpleEntry<>(key.get(), value.get());
						} else {
							System.err.println("unknown value?! " + entry.getValue());
						}
					}
					return (Map.Entry<K, V>) null;
				})
				.filter(e -> e != null);
	}

	@Override
	public boolean equals(Object obj) {
		return entrySet().equals(obj);
	}

	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		return Optional.ofNullable(map.get(keyBroker.toBB((K) key)))
				.flatMap(bb -> rewind(bb, valueBroker.fromBB(bb)))
				.orElse(null);
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
		return map.keySet().stream()
				.map(keyBuffer -> rewind(keyBuffer, keyBroker.fromBB(keyBuffer)))
				.filter(o -> o.isPresent())
				.map(o -> o.get())
				.collect(Collectors.toSet());
	}

	@Override
	public V put(K key, V value) {
		ByteBuffer oldBuffer = map.put(keyBroker.toBB(key), valueBroker.toBB(value));
		return oldBuffer == null ? null
				: rewind(oldBuffer, valueBroker.fromBB(oldBuffer))
						.orElseThrow(() -> new IllegalStateException("Key collision"));
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> otherMap) {
		otherMap.entrySet().stream()
				.forEach(entry -> map.put(keyBroker.toBB(entry.getKey()), valueBroker.toBB(entry.getValue())));
	}

	@Override
	public V remove(Object key) {
		@SuppressWarnings("unchecked")
		ByteBuffer oldBuffer = map.remove(keyBroker.toBB((K) key));
		return oldBuffer == null ? null
				: rewind(oldBuffer, valueBroker.fromBB(oldBuffer))
						.orElseThrow(() -> new IllegalStateException("Key collision"));
	}

	private <T> T rewind(ByteBuffer bb, T value) {
		bb.rewind();
		return value;
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public String toString() {
		Iterator<Entry<K, V>> i = entrySet().iterator();
		if (!i.hasNext()) {
			return "{}";
		}

		StringBuilder sb = new StringBuilder();
		sb.append('{');
		for (;;) {
			Entry<K, V> e = i.next();
			K key = e.getKey();
			V value = e.getValue();
			sb.append(key == this ? "(this Map)" : key);
			sb.append('=');
			sb.append(value == this ? "(this Map)" : value);
			if (!i.hasNext()) {
				return sb.append('}').toString();
			}
			sb.append(',').append(' ');
		}
	}

	synchronized public void update(K key, Function<V, V> fn) {
		put(key, fn.apply(get(key)));
	}

	@Override
	public Collection<V> values() {
		return entryStream().map(o -> o.getValue())
				.collect(Collectors.toSet());
	}
}