package net.soliddesign.map;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.gson.Gson;

public class PersistentBufferMap implements Map<ByteBuffer, ByteBuffer>, Closeable {

	// so that we can evolve the file format
	static final private int VERSION = 1;

	final private BigByteBuffer buf;
	/** start of index */
	final private long indexPointer;
	/** number of entries in index */
	final private int indexSize;

	public PersistentBufferMap(File fileName, int indexSize) throws IOException {
		if (indexSize < 0) {
			// read index from file
			if (!fileName.exists())
				throw new FileNotFoundException("PersistentMap file not found:" + fileName);
			buf = new BigByteBuffer(fileName);
			int version = buf.getInt();
			if (VERSION != version) {
				throw new IllegalStateException("Invalid version:" + version);
			}
			this.indexSize = buf.getInt();
			indexPointer = buf.position();
		} else {
			// create new file with index of side indexSize
			if (fileName.exists() && fileName.length() > 0)
				throw new IllegalStateException("PersistentMap file already exists:" + fileName);
			buf = new BigByteBuffer(fileName);
			this.indexSize = indexSize;
			buf.putInt(VERSION);
			buf.putInt(indexSize);
			indexPointer = buf.position();
			clear();
		}
	}

	private int bucket(ByteBuffer key) {
		int h = 1;
		for (int i = key.limit() - 1; i >= 0; i--) {
			h = 31 * h + (int) key.get(i);
		}
		return (0x7FFFFFFF & h) % indexSize;
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
	public Set<java.util.Map.Entry<ByteBuffer, ByteBuffer>> entrySet() {
		return stream().collect(Collectors.toSet());
	}

	public Iterator<java.util.Map.Entry<ByteBuffer, ByteBuffer>> entrySetIterator() {
		return new Iterator<java.util.Map.Entry<ByteBuffer, ByteBuffer>>() {
			long index = 0;
			long item = -1;
			java.util.Map.Entry<ByteBuffer, ByteBuffer> next;

			{
				next = calc();
			}

			private java.util.Map.Entry<ByteBuffer, ByteBuffer> calc() {
				while (item < 0) {
					if (index >= indexSize) {
						return null;
					}
					buf.position(indexPointer + index++ * 8);
					item = buf.getLong();
				}
				buf.position(item);
				Entry<ByteBuffer, ByteBuffer> e = new AbstractMap.SimpleEntry<ByteBuffer, ByteBuffer>(read(), read());
				item = buf.getLong();
				return e;
			}

			@Override
			public boolean hasNext() {
				return next != null;
			}

			@Override
			public java.util.Map.Entry<ByteBuffer, ByteBuffer> next() {
				java.util.Map.Entry<ByteBuffer, ByteBuffer> o = next;
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
	public ByteBuffer get(Object key) {
		// read the index entry
		int bucket = bucket((ByteBuffer) key);
		buf.position(indexPointer + 8 * bucket);
		long listPointer = buf.getLong();
		while (true) {
			if (listPointer < 0) {
				return null;
			}
			// iterate through the bucket of pairs
			buf.position(listPointer);
			ByteBuffer candidate = read();
			ByteBuffer value = read();
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
	public Set<ByteBuffer> keySet() {
		return stream().map(x -> x.getKey()).collect(Collectors.toSet());
	}

	@Override
	public ByteBuffer put(ByteBuffer key, ByteBuffer value) {
		long offset = buf.length();
		buf.position(offset);
		buf.putBuffer(key);
		buf.putBuffer(value);
		buf.putLong(-1);
		// read the index entry
		buf.position(indexPointer + 8 * bucket(key));
		long listPointer = buf.getLong();
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
			ByteBuffer candidate = read();
			ByteBuffer old = read();
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
	public void putAll(Map<? extends ByteBuffer, ? extends ByteBuffer> m) {
		m.forEach((k, v) -> put(k, v));
	}

	private ByteBuffer read() {
		return buf.getBuffer();
	}

	@Override
	public ByteBuffer remove(Object key) {
		// read the index entry
		buf.position(indexPointer + 8 * bucket((ByteBuffer) key));
		long listPointer = buf.getLong();
		while (true) {
			if (listPointer < 0) {
				return null;
			}
			long oldPointer = buf.position() - 8;
			buf.position(listPointer);
			ByteBuffer candidate = read();
			ByteBuffer value = read();
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

	public Stream<java.util.Map.Entry<ByteBuffer, ByteBuffer>> stream() {
		Iterable<java.util.Map.Entry<ByteBuffer, ByteBuffer>> i = () -> entrySetIterator();
		return StreamSupport.stream(i.spliterator(), false);
	}

	@Override
	public String toString() {
		return entrySet().toString();
	}

	@Override
	public Collection<ByteBuffer> values() {
		return stream().map(x -> x.getValue()).collect(Collectors.toList());
	}

	public <K, V> CloseableMap<K, V> adapt(Function<K, ByteBuffer> fromKey, Function<ByteBuffer, K> toKey,
			Function<V, ByteBuffer> fromValue, Function<ByteBuffer, V> toValue) {
		return new BufferMapAdapter<K, V>(this, fromKey, toKey, fromValue, toValue);
	}

	static public class StringMap extends BufferMapAdapter<String, String> {
		private StringMap(Map<ByteBuffer, ByteBuffer> map) {
			super(map, s -> {

				ByteBuffer bb = ByteBuffer.allocate(s.length() * 2);
				bb.asCharBuffer().append(s);
				return bb;
			} , b -> b == null ? null : b.asCharBuffer().toString(), s -> {
				ByteBuffer bb = ByteBuffer.allocate(s.length() * 2);
				bb.asCharBuffer().append(s);
				return bb;
			} , b -> b == null ? null : b.asCharBuffer().toString());

		}

		public static StringMap create(Map<ByteBuffer, ByteBuffer> map) {
			return new StringMap(map);
		}
	}

	static public class GsonMap<K, V> extends BufferMapAdapter<K, V> {
		private GsonMap(Map<ByteBuffer, ByteBuffer> map, Class<K> keyClass, Class<V> valueClass) {
			this(new Gson(), map, keyClass, valueClass);
		}

		private GsonMap(Gson g, Map<ByteBuffer, ByteBuffer> map, Class<K> keyClass, Class<V> valueClass) {
			super(map, o -> {
				String s = g.toJson(o);
				ByteBuffer bb = ByteBuffer.allocate(s.length() * 2);
				bb.asCharBuffer().append(s);
				return bb;
			} , b -> b == null ? null : g.fromJson(b.asCharBuffer().toString(), keyClass), o -> {
				String s = g.toJson(o);
				ByteBuffer bb = ByteBuffer.allocate(s.length() * 2);
				bb.asCharBuffer().append(s);
				return bb;
			} , b -> b == null ? null : g.fromJson(b.asCharBuffer().toString(), valueClass));
		}

		public static <K, V> GsonMap<K, V> create(Map<ByteBuffer, ByteBuffer> map, Class<K> keyClass,
				Class<V> valueClass) {
			// TODO Auto-generated method stub
			return new GsonMap<K, V>(map, keyClass, valueClass);
		}
	}
}
