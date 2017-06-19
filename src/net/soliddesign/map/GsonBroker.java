package net.soliddesign.map;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Optional;

import com.google.gson.Gson;

public class GsonBroker<T> implements BBBroker<T> {
	final static private ThreadLocal<Gson> gson = ThreadLocal.withInitial(() -> new Gson());
	final private Class<T> cls;

	public GsonBroker(Class<T> cls) {
		this.cls = cls;
	}

	@Override
	public Optional<T> fromBB(ByteBuffer b) {
		int len = b.getInt();
		ByteBuffer b2 = (ByteBuffer) b.slice().limit(len);
		b.position(b.position() + len);
		Optional<CharBuffer> opt = Optional.of(Charset.forName("UTF-16").decode(b2));
		return opt.map(o -> gson.get().fromJson(o.toString(), cls));
	}

	@Override
	public ByteBuffer toBB(T v) {
		String s = gson.get().toJson(v);
		return (ByteBuffer) ByteBuffer.allocate(s.length() * 2 + 2 + Integer.BYTES)
				.putInt(s.length() * 2)
				.put((ByteBuffer) Charset.forName("UTF-16").encode(s).position(2))
				.flip();
	}

}
