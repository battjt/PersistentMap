package net.soliddesign.map;

import java.nio.ByteBuffer;
import java.util.Optional;

import com.google.gson.Gson;

public class GsonBroker<T> implements BBBroker<T> {
	final static private ThreadLocal<Gson> gson = ThreadLocal.withInitial(() -> new Gson());
	final private Class<T> cls;

	public GsonBroker(Class<T> cls) {
		this.cls = cls;
	}

	@Override
	public Optional<T> fromBB(ByteBuffer bb) {
		return Optional.ofNullable(bb).map(o -> gson.get().fromJson(o.asCharBuffer().toString(), cls));
	}

	@Override
	public ByteBuffer toBB(T v) {
		String s = gson.get().toJson(v);
		ByteBuffer bb = ByteBuffer.allocate(s.length() * 2);
		bb.asCharBuffer().append(s);
		return bb;
	}

}
