package net.soliddesign.map;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Optional;

public interface BBBroker<T> {

	static final public BBBroker<Integer> intBroker = new BBBroker<Integer>() {

		@Override
		public Optional<Integer> fromBB(ByteBuffer bb) {
			if (bb.remaining() >= Integer.BYTES) {
				return Optional.of(bb.getInt());
			} else {
				return Optional.empty();
			}
		}

		@Override
		public ByteBuffer toBB(Integer v) {
			return (ByteBuffer) ByteBuffer.allocate(Integer.BYTES).putInt(v).flip();
		}
	};
	static final public BBBroker<Long> longBroker = new BBBroker<Long>() {

		@Override
		public Optional<Long> fromBB(ByteBuffer bb) {
			if (bb.remaining() >= Long.BYTES) {
				return Optional.of(bb.getLong());
			} else {
				return Optional.empty();
			}
		}

		@Override
		public ByteBuffer toBB(Long v) {
			return (ByteBuffer) ByteBuffer.allocate(Long.BYTES).putLong(v).flip();
		}
	};

	static final public BBBroker<String> stringBroker = new BBBroker<String>() {

		@Override
		public Optional<String> fromBB(ByteBuffer b) {
			int len = b.getInt();
			ByteBuffer b2 = (ByteBuffer) b.slice().limit(len);
			b.position(b.position() + len);
			return Optional.of(Charset.forName("UTF-16").decode(b2).toString());
		}

		@Override
		public ByteBuffer toBB(String s) {
			ByteBuffer ebuf = Charset.forName("UTF-16").encode(s);
			return (ByteBuffer) ByteBuffer.allocate(ebuf.limit() + Integer.BYTES).putInt(ebuf.limit())
					.put(ebuf).flip();
		}
	};

	public static ByteBuffer toBB(ByteBuffer... buffers) {
		int size = 0;
		for (ByteBuffer bb : buffers) {
			size += bb.remaining();
		}
		ByteBuffer res = ByteBuffer.allocate(size);
		for (ByteBuffer bb : buffers) {
			res.put(bb);
		}
		res.flip();
		return res;
	}

	public Optional<T> fromBB(ByteBuffer bb);

	public ByteBuffer toBB(T v);
}
