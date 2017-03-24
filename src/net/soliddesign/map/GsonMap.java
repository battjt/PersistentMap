package net.soliddesign.map;

import java.nio.ByteBuffer;
import java.util.Map;

public class GsonMap<K, V> extends BufferMapAdapter<K, V> {

	public GsonMap(Map<ByteBuffer, ByteBuffer> map, String name, Class<K> keyClass, Class<V> valueClass) {
		super(map, name, new MagicBroker<>("GSON", new GsonBroker<>(keyClass)), new GsonBroker<>(valueClass));
	}
}