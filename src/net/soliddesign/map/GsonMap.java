package net.soliddesign.map;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;

import com.google.gson.Gson;

public class GsonMap<K, V> extends BufferMapAdapter<K, V> {
	final static private ThreadLocal<Gson> gson = ThreadLocal.withInitial(() -> new Gson());

	public static <K, V> GsonMap<K, V> create(Map<ByteBuffer, ByteBuffer> map, Class<K> keyClass, Class<V> valueClass) {
		return new GsonMap<>(map, keyClass, valueClass);
	}

	private static <T> Optional<T> fromJson(ByteBuffer b, Class<T> cls) {
		return Optional.ofNullable(b).map(o -> gson.get().fromJson(o.asCharBuffer().toString(), cls));
	}

	private static ByteBuffer toJson(Object o) {
		String s = gson.get().toJson(o);
		ByteBuffer bb = ByteBuffer.allocate(s.length() * 2);
		bb.asCharBuffer().append(s);
		return bb;
	}

	private GsonMap(Map<ByteBuffer, ByteBuffer> map, Class<K> keyClass, Class<V> valueClass) {
		super(map, k -> toJson(k), b -> fromJson(b, keyClass), v -> toJson(v), b -> fromJson(b, valueClass));
	}
}