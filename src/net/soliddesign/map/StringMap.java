package net.soliddesign.map;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;

public class StringMap extends BufferMapAdapter<String, String> {
	public static StringMap create(Map<ByteBuffer, ByteBuffer> map) {
		return new StringMap(map);
	}

	private StringMap(Map<ByteBuffer, ByteBuffer> map) {
		super(map, s -> {
			ByteBuffer bb = ByteBuffer.allocate(s.length() * 2);
			bb.asCharBuffer().append(s);
			return bb;
		}, b -> b == null ? null : Optional.of(b.asCharBuffer().toString()), s -> {
			ByteBuffer bb = ByteBuffer.allocate(s.length() * 2);
			bb.asCharBuffer().append(s);
			return bb;
		}, b -> b == null ? null : Optional.of(b.asCharBuffer().toString()));
	}
}