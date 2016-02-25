package net.soliddesign.map;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.gson.Gson;

public class PersistentBufferMap implements Map<ByteBuffer, ByteBuffer>, Closeable {

	// so that we can evolve the file format
	static final private int VERSION = 1;

	// maximum number of bytes to sample from key for hash value.
	private static final int SAMPLES = 128;

	final private BigByteBuffer buf;
	/** start of index */
	final private long indexPointer;
	/** number of entries in index */
	final private int indexSize;

	/**
	 * 
	 * @param fileName
	 * @param indexSize
	 *            Expected size of Map. -1 to read existing map from file.
	 * @throws IOException
	 */
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
		// don't sample more than SAMPLES
		int step = key.limit() > SAMPLES ? key.limit() / SAMPLES : 1;
		for (int i = key.limit() - 1; i >= 0; i = i - step) {
			h = 31 * h + (int) key.get(i);
		}
		return (0x7FFFFFFF & h) % indexSize;
	}

	@Override
	/** Remove all contents of this map. */
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

	public Stream<java.util.Map.Entry<ByteBuffer, ByteBuffer>> stream() {
		return IntStream.range(0, indexSize).sequential().mapToObj(i -> {
			Stream.Builder<java.util.Map.Entry<ByteBuffer, ByteBuffer>> sb = Stream.builder();
			long offset = indexPointer + Long.BYTES * i;
			buf.position(offset);
			offset = buf.getLong();
			while (offset > 0) {
				buf.position(offset);
				sb.add(new AbstractMap.SimpleEntry<ByteBuffer, ByteBuffer>(buf.getBuffer(), buf.getBuffer()));
				offset = buf.getLong();
			}
			return sb.build();
		}).flatMap(x -> x);
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof Map && entrySet().equals(((Map<?, ?>) obj).entrySet());
	}

	@Override
	public ByteBuffer get(Object key) {
		// read the index entry
		int bucket = bucket((ByteBuffer) key);
		buf.position(indexPointer + Long.BYTES * bucket);
		long listPointer = buf.getLong();
		while (listPointer >= 0) {
			// iterate through the bucket of pairs
			buf.position(listPointer);
			ByteBuffer candidate = buf.getBuffer();
			ByteBuffer value = buf.getBuffer();
			if (candidate.equals(key)) {
				return value;
			}
			listPointer = buf.getLong();
		}
		return null;
	}

	@Override
	public int hashCode() {
		return stream().mapToInt(b -> b.hashCode()).sum();
	}

	@Override
	public boolean isEmpty() {
		long index = 0;
		long item = -1;
		while (item < 0) {
			if (index >= indexSize) {
				return true;
			}
			buf.position(indexPointer + index++ * Long.BYTES);
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
		buf.position(indexPointer + Long.BYTES * bucket(key));
		long listPointer = buf.getLong();
		while (true) {
			if (listPointer < 0) {
				// no replacement, just append
				buf.position(buf.position() - Long.BYTES);
				buf.putLong(offset);
				return null;
			}
			long oldPointer = buf.position() - Long.BYTES;
			// iterate through the bucket of pairs
			buf.position(listPointer);
			ByteBuffer candidate = buf.getBuffer();
			ByteBuffer old = buf.getBuffer();
			if (candidate.equals(key)) {
				// replacing old value in linked list
				long next = buf.getLong();
				buf.position(oldPointer);
				buf.putLong(offset);
				buf.position(offset - Long.BYTES);
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

	@Override
	public ByteBuffer remove(Object key) {
		// read the index entry
		buf.position(indexPointer + Long.BYTES * bucket((ByteBuffer) key));
		long listPointer = buf.getLong();
		while (true) {
			if (listPointer < 0) {
				return null;
			}
			long oldPointer = buf.position() - Long.BYTES;
			buf.position(listPointer);
			ByteBuffer candidate = buf.getBuffer();
			ByteBuffer value = buf.getBuffer();
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
				buf.position(indexPointer + index++ * Long.BYTES);
				item = buf.getLong();
			}
			buf.position(item);
			count++;
			buf.skip();
			buf.skip();
			item = buf.getLong();
		}
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
