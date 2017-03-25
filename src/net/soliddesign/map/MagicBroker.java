package net.soliddesign.map;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Optional;

public class MagicBroker<T> implements BBBroker<T> {
	final private ByteBuffer magic;

	final private BBBroker<T> broker;

	public MagicBroker(String magic, BBBroker<T> broker) {
		this.broker = broker;
		this.magic = ((ByteBuffer) Charset.forName("UTF-16").encode(magic).position(2)).slice();
	}

	@Override
	public Optional<T> fromBB(ByteBuffer bb) {
		magic.rewind();
		int length = bb.getInt();
		if (length == magic.limit()
				&& ((ByteBuffer) bb.slice().limit(length)).compareTo(magic) == 0) {
			bb.position(bb.position() + length);
			return broker.fromBB(bb);
		}
		return Optional.empty();
	}

	@Override
	public ByteBuffer toBB(T v) {
		ByteBuffer bb = broker.toBB(v);
		magic.rewind();
		return (ByteBuffer) ByteBuffer.allocate(bb.limit() + magic.limit() + Integer.BYTES).putInt(magic.limit())
				.put(magic).put(bb).flip();
	}

}
