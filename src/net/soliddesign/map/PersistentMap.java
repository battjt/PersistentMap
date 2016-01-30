package net.soliddesign.map;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Simple disk backed map.
 *
 * Roughly this is a hashtable with linked lists. The index size is specified at
 * creation. Each data entry is (key, value, pointer to next). If pointer to
 * next is -1, then this is the end of the linked list.
 *
 * <pre>
 * File format:
 *  version
 *  number of entries in index
 *  index entry 0
 *  index entry 1
 *  index entry 2
 *  ...
 *  {key bytes}{value bytes}{next in linked list}
 *
 * </pre>
 *
 * entrySet(), values(), and keySet() are horrible, because they load the whole
 * DB into memory.
 *
 * There is no garbage collection or index resizing. It must be done manually.
 * <code>
 *
    try (PersistentStringMap mapa = new PersistentStringMap(filea, 5);) {
      for (int i = 0; i < 20; i++) {
        mapa.put(String.valueOf((char) ('a' + i)), String.valueOf(i));
      }
      try (PersistentStringMap mapb = new PersistentStringMap(fileb, mapa.size());) {
        mapb.putAll(mapa);
      }
    }
 *</code>
 *
 * The file is resized at 150% of what is needed to avoid frequent remapping the
 * memory buffer. This will lead to extra junk at end of file.
 * 
 * TODO: make threadable by keeping position in Map or on stack.
 * 
 * @author YYYB751
 *
 */
public class PersistentMap<K, V> implements Map<K, V>, Closeable {
	static public PersistentMap<String, String> stringMap(File file, int indexes) throws IOException {
		return new PersistentMap<>(file, indexes, k -> k.getBytes(), v -> v.getBytes(), b -> new String(b),
				b -> new String(b));
	}

	static public PersistentMap<Object, Object> gsonMap(File file) {
		return null;
	}

	// so that we can evolve the file format
	static final private int VERSION = 1;

	final private BigByteBuffer buf;
	/** start of index */
	final private long indexPointer;
	/** number of entries in index */
	final private int indexSize;

	final private Function<K, byte[]> marshalKey;
	final private Function<V, byte[]> marshalValue;
	final private Function<byte[], K> unmarshalKey;
	final private Function<byte[], V> unmarshalValue;

	public PersistentMap(File fileName, int indexSize, Function<K, byte[]> marshalKey, Function<V, byte[]> marshalValue,
			Function<byte[], K> unmarshalKey, Function<byte[], V> unmarshalValue) throws IOException {
		buf = new BigByteBuffer(fileName);
		if (indexSize < 0) {
			int version = buf.getInt();
			if (VERSION != version) {
				throw new IllegalStateException("Invalid version:" + version);
			}
			this.indexSize = buf.getInt();
			indexPointer = buf.position();
		} else {
			this.indexSize = indexSize;
			buf.putInt(VERSION);
			buf.putInt(indexSize);
			indexPointer = buf.position();
			clear();
		}
		this.marshalKey = marshalKey;
		this.marshalValue = marshalValue;
		this.unmarshalKey = unmarshalKey;
		this.unmarshalValue = unmarshalValue;
	}

	private int bucket(Object key) {
		return (0x7FFFFFFF & key.hashCode()) % indexSize;
	}

	@Override
	public void clear() {
		buf.position(indexPointer);
		for (int i = 0; i < indexSize; i++) {
			buf.putLong(-1);
		}
	}

	@Override
	public void close() throws IOException {
		// leave junk at end of file. Unable to truncate a memory mapped file.
		// Unable to close memory map.
		// file.setLength(eof);
		buf.close();
	}

	@Override
	public boolean containsKey(Object key) {
		return get(key) != null;
	}

	@Override
	public boolean containsValue(Object value) {
		return stream().anyMatch(e -> e.getValue().equals(value));
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return stream().collect(Collectors.toSet());
	}

	public Iterator<java.util.Map.Entry<K, V>> entrySetIterator() {
		return new Iterator<java.util.Map.Entry<K, V>>() {
			long index = 0;
			long item = -1;
			java.util.Map.Entry<K, V> next;

			{
				next = calc();
			}

			private java.util.Map.Entry<K, V> calc() {
				while (item < 0) {
					if (index >= indexSize) {
						return null;
					}
					buf.position(indexPointer + index++ * 8);
					item = buf.getLong();
				}
				buf.position(item);
				Entry<K, V> e = new AbstractMap.SimpleEntry<K, V>(read(unmarshalKey), read(unmarshalValue));
				item = buf.getLong();
				return e;
			}

			@Override
			public boolean hasNext() {
				return next != null;
			}

			@Override
			public java.util.Map.Entry<K, V> next() {
				java.util.Map.Entry<K, V> o = next;
				next = calc();
				return o;
			}
		};
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof Map && entrySet().equals(((Map<?, ?>) obj).entrySet());
	}

	@Override
	public V get(Object key) {
		// read the index entry
		int bucket = bucket(key);
		buf.position(indexPointer + 8 * bucket);
		long listPointer = buf.getLong();
		// System.err.println("GET key:" + key + " bucket:" + bucket + " list:"
		// +
		// (indexPointer + 8 * bucket)
		// + " listPointer:" + listPointer);
		while (true) {
			if (listPointer < 0) {
				return null;
			}
			// iterate through the bucket of pairs
			buf.position(listPointer);
			K candidate = read(unmarshalKey);
			V value = read(unmarshalValue);
			if (candidate.equals(key)) {
				return value;
			}
			listPointer = buf.getLong();
		}
	}

	@Override
	public int hashCode() {
		return entrySet().hashCode();
	}

	@Override
	public boolean isEmpty() {
		long index = 0;
		long item = -1;
		while (item < 0) {
			if (index >= indexSize) {
				return true;
			}
			buf.position(indexPointer + index++ * 8);
			item = buf.getLong();
		}
		return false;
	}

	@Override
	public Set<K> keySet() {
		return stream().map(x -> x.getKey()).collect(Collectors.toSet());
	}

	@Override
	public V put(K key, V value) {
		byte[] keyBytes = marshalKey.apply(key);
		byte[] valueBytes = marshalValue.apply(value);
		long offset = buf.length();
		buf.position(offset);
		buf.putInt(keyBytes.length);
		buf.putBytes(keyBytes);
		buf.putInt(valueBytes.length);
		buf.putBytes(valueBytes);
		buf.putLong(-1);
		// read the index entry
		buf.position(indexPointer + 8 * bucket(key));
		long listPointer = buf.getLong();
		System.err.println("PUT key:" + key + " bucket:" + bucket(key) + " list:" + (indexPointer + 8 * bucket(key))
				+ " listPointer:" + listPointer);
		while (true) {
			if (listPointer < 0) {
				// no replacement, just append
				buf.position(buf.position() - 8);
				buf.putLong(offset);
				return null;
			}
			long oldPointer = buf.position() - 8;
			// iterate through the bucket of pairs
			buf.position(listPointer);
			K candidate = read(unmarshalKey);
			V old = read(unmarshalValue);
			if (candidate.equals(key)) {
				// replacing old value in linked list
				long next = buf.getLong();
				buf.position(oldPointer);
				buf.putLong(offset);
				buf.position(offset - 8);
				buf.putLong(next);
				return old;
			}
			listPointer = buf.getLong();
		}
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		m.forEach((k, v) -> put(k, v));
	}

	private <T> T read(Function<byte[], T> unmarshaller) {
		return unmarshaller.apply(buf.getBytes(buf.getInt()));
	}

	@Override
	public V remove(Object key) {
		// read the index entry
		buf.position(indexPointer + 8 * bucket(key));
		long listPointer = buf.getLong();
		while (true) {
			if (listPointer < 0) {
				return null;
			}
			long oldPointer = buf.position() - 8;
			buf.position(listPointer);
			K candidate = read(unmarshalKey);
			V value = read(unmarshalValue);
			if (candidate.equals(key)) {
				long next = buf.getLong();
				buf.position(oldPointer);
				buf.putLong(next);
				return value;
			}
			listPointer = buf.getLong();
		}
	}

	@Override
	/** will return invalid values for > Integer.MAX_VALUE */
	public int size() {
		long index = 0;
		long item = -1;
		int count = 0;
		while (true) {
			while (item < 0) {
				if (index >= indexSize) {
					return count;
				}
				buf.position(indexPointer + index++ * 8);
				item = buf.getLong();
			}
			buf.position(item);
			count++;
			skip();
			skip();
			item = buf.getLong();
		}
	}

	private void skip() {
		buf.position(buf.position() + buf.getInt() + 4);
	}

	public Stream<java.util.Map.Entry<K, V>> stream() {
		Iterable<java.util.Map.Entry<K, V>> i = () -> entrySetIterator();
		return StreamSupport.stream(i.spliterator(), false);
	}

	@Override
	public String toString() {
		return entrySet().toString();
	}

	@Override
	public Collection<V> values() {
		return stream().map(x -> x.getValue()).collect(Collectors.toList());
	}
}
