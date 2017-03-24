package net.soliddesign.map;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Optional;

public interface BBBroker<T> {

	static final public BBBroker<Integer> intBroker = new BBBroker<Integer>() {

		@Override
		public Optional<Integer> fromBB(ByteBuffer bb) {
			return bb.remaining() > Integer.SIZE ? Optional.of(bb.getInt()) : Optional.empty();
		}

		@Override
		public ByteBuffer toBB(Integer v) {
			return (ByteBuffer) ByteBuffer.allocate(Integer.SIZE).putInt(v).flip();
		}
	};
	static final public BBBroker<Long> longBroker = new BBBroker<Long>() {

		@Override
		public Optional<Long> fromBB(ByteBuffer bb) {
			return bb.remaining() > Long.SIZE ? Optional.of(bb.getLong()) : Optional.empty();
		}

		@Override
		public ByteBuffer toBB(Long v) {
			return (ByteBuffer) ByteBuffer.allocate(Long.SIZE).putLong(v).flip();
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
			return (ByteBuffer) ByteBuffer.allocate(s.length() * 2 + 2 + Integer.SIZE)
					.putInt(s.length() * 2)
					.put((ByteBuffer) Charset.forName("UTF-16").encode(s).position(2))
					.flip();
		}
	};

	public Optional<T> fromBB(ByteBuffer bb);

	public ByteBuffer toBB(T v);
}
