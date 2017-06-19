package net.soliddesign.map;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class MapIndexTest {

	@Test
	@Ignore
	public void bbTest() throws Exception {
		long count = 4194303;
		long offset = 3;
		File file = File.createTempFile("bbb.test.", ".bbb");
		try (BBBuffer bbb = BBBuffer.create(file)) {
			bbb.position(offset);
			for (long value = 0; value < count; value++) {
				bbb.putLong(value);
			}
			for (long value = count - 1; value >= 0; value--) {
				bbb.position(offset + value * Long.BYTES);
				long v = bbb.getLong();
				assertEquals(String.format("%X", v), value, v);
			}
			bbb.position(offset);
			for (long value = 0; value < count; value++) {
				long v = bbb.getLong();
				assertEquals(String.format("%X", v), value, v);
			}

		}
		try (BBBuffer bbb = BBBuffer.create(file)) {
			bbb.position(offset);
			for (long value = 0; value < count; value++) {
				long v = bbb.getLong();
				assertEquals(String.format("%X", v), value, v);
			}
			for (long value = count - 1; value >= 0; value--) {
				bbb.position(offset + value * Long.BYTES);
				long v = bbb.getLong();
				assertEquals(String.format("%X", v), value, v);
			}
		}
	}

	@Test
	public void brokers() {
		ByteBuffer bb = ByteBuffer.allocate(512);
		bb.put(BBBroker.stringBroker.toBB("TEST1"));
		bb.put(BBBroker.stringBroker.toBB("TEST2"));

		bb.position(0);
		assertEquals("TEST1", BBBroker.stringBroker.fromBB(bb).get());
		assertEquals("TEST2", BBBroker.stringBroker.fromBB(bb).get());
	}

	@Test
	public void bucketTest() throws Exception {
		File f = File.createTempFile("test.", ".mapdb");
		try (PersistentBufferMap pers = new PersistentBufferMap(f, 7)) {
			Comparator<String> comparator = (a, b) -> a.compareTo(b);
			MapIndex<String, String> index = new MapIndex<>(pers, "words",
					comparator, BBBroker.stringBroker, BBBroker.stringBroker);
			for (int i = 0; i < 20; i++) {
				index.put("k" + (i % 3), "v" + i);
			}
		}
	}

	@Test
	@Ignore
	public void hugePersistentBufferMapTest() throws Exception {
		File f = File.createTempFile("test.huge.", ".mapdb");
		int count = 5000000;

		try (PersistentBufferMap pers = new PersistentBufferMap(f, count)) {
			for (int i = 0; i < count; i++) {
				pers.put((ByteBuffer) ByteBuffer.allocate(Long.BYTES).putLong(i).flip(),
						(ByteBuffer) ByteBuffer.allocate(Long.BYTES * 2).putLong(-1L).putLong(i).flip());
				if (i % 100000 == 0) {
					System.err.format("%f%%\n", 100 * i / (double) count);
					for (long j = 0; j <= i; j++) {
						Assert.assertEquals(
								ByteBuffer.allocate(Long.BYTES * 2).putLong(-1L).putLong(j).flip(),
								pers.get(ByteBuffer.allocate(Long.BYTES).putLong(j).flip()));
					}
				}
			}
		}
	}

	@Test
	public void lotsaObjects() throws Exception {
		File f = File.createTempFile("test.", ".mapdb");
		int count = (int) Files.lines(Paths.get("/usr/share/dict/words")).count();
		System.err.println("words found:" + count);
		long start = System.currentTimeMillis();

		try (PersistentBufferMap pers = new PersistentBufferMap(f, 4)) {
			// Map<ByteBuffer, ByteBuffer> pers = new HashMap<>(count / 10);
			Comparator<String> comparator = (a, b) -> a.compareTo(b);
			MapIndex<String, String> index = new MapIndex<>(pers, "words",
					comparator, BBBroker.stringBroker, BBBroker.stringBroker);
			Files.lines(Paths.get("/usr/share/dict/words"))
					// .limit(4000)
					.forEach(w -> {
						try {
							index.put(new StringBuffer(w).reverse().toString(), w);
						} catch (Exception e) {
							throw new Error(w, e);
						}
					});
			index.find("boom", "box").forEach(x -> System.err.println("Boom: " + x));
			Assert.assertEquals(count, index.find(null, null).count());
			System.err.println("check balance");
		}

		try (PersistentBufferMap pers = new PersistentBufferMap(f, -1)) {
			MapIndex<String, String> index = new MapIndex<>(pers, "words",
					String.CASE_INSENSITIVE_ORDER, BBBroker.stringBroker, BBBroker.stringBroker);
			index.find("A", "B").forEach(x -> System.err.println(x));
		}
		Assert.assertTrue("Too slow.", System.currentTimeMillis() - start < 30000);
	}

	@Test
	public void replaceTest() throws Exception {
		File f = File.createTempFile("test.replace.", ".mapdb");
		int count = 50;

		try (PersistentBufferMap pers = new PersistentBufferMap(f, count)) {
			ByteBuffer key = (ByteBuffer) ByteBuffer.allocate(Long.BYTES).putLong(5555).flip();
			ByteBuffer value1 = (ByteBuffer) ByteBuffer.allocate(Long.BYTES * 2).putLong(-1L).putLong(1).flip();
			ByteBuffer value2 = (ByteBuffer) ByteBuffer.allocate(Long.BYTES * 2).putLong(-2L).putLong(1).flip();
			pers.put(key, value1);
			Assert.assertEquals(value1.rewind(), pers.put((ByteBuffer) key.rewind(), value2));
		}
	}
}
