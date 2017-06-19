package net.soliddesign.map;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

public class BBBrokerTest {
	@Test
	public void testDoubleMagic() throws Exception {
		ByteBuffer bb = ByteBuffer.allocate(1024);
		MagicBroker<String> mb = new MagicBroker<>("MAGIC", new MagicBroker<>("STR", BBBroker.stringBroker));
		bb.put(mb.toBB("red"));
		bb.put(mb.toBB("blue"));
		bb.put(mb.toBB("green"));
		bb.flip();
		Assert.assertEquals("red", mb.fromBB(bb).get());
		Assert.assertEquals("blue", mb.fromBB(bb).get());
		Assert.assertEquals("green", mb.fromBB(bb).get());
	}

	@Test
	public void testMagic() throws Exception {
		ByteBuffer bb = ByteBuffer.allocate(1024);
		MagicBroker<String> mb = new MagicBroker<>("MAGIC", BBBroker.stringBroker);
		bb.put(mb.toBB("red"));
		bb.put(mb.toBB("blue"));
		bb.put(mb.toBB("green"));
		bb.flip();
		Assert.assertEquals("red", mb.fromBB(bb).get());
		Assert.assertEquals("blue", mb.fromBB(bb).get());
		Assert.assertEquals("green", mb.fromBB(bb).get());
	}

	@Test
	public void testString() throws Exception {
		ByteBuffer bb = ByteBuffer.allocate(1024);
		bb.put(BBBroker.stringBroker.toBB("red"));
		bb.put(BBBroker.stringBroker.toBB("blue"));
		bb.put(BBBroker.stringBroker.toBB("green"));
		bb.flip();
		Assert.assertEquals("red", BBBroker.stringBroker.fromBB(bb).get());
		Assert.assertEquals("blue", BBBroker.stringBroker.fromBB(bb).get());
		Assert.assertEquals("green", BBBroker.stringBroker.fromBB(bb).get());
	}
}
