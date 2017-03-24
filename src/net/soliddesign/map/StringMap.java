package net.soliddesign.map;

import java.nio.ByteBuffer;
import java.util.Map;

public class StringMap extends BufferMapAdapter<String, String> {
	public StringMap(Map<ByteBuffer, ByteBuffer> map, String name) {
		super(map, name, BBBroker.stringBroker, BBBroker.stringBroker);
	}
}