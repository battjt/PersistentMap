package net.soliddesign.map;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Collectors;

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
				index.put("k" + (i % 5), "v" + i);
			}
			System.err.println(index);
			System.err.println(index.find("k3", "k5").collect(Collectors.toList()));
			Assert.assertEquals(8, index.find("k3", "k5").count());
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
		File file = File.createTempFile("lotsaobjects.", ".mapdb");

		int count = (int) Files.lines(Paths.get("/usr/share/dict/words")).count();
		System.err.println("words found:" + count);
		long start = System.currentTimeMillis();

		try (PersistentBufferMap pers = new PersistentBufferMap(file, count)) {
			Comparator<String> comparator = (a, b) -> a.compareTo(b);
			MapIndex<String, String> index = new MapIndex<>(pers, "words",
					comparator, BBBroker.stringBroker, BBBroker.stringBroker);
			System.err.println("init:" + (System.currentTimeMillis() - start));
			Files.lines(Paths.get("/usr/share/dict/words"))
					.forEach(w -> {
						try {
							index.put(w, w);
						} catch (Exception e) {
							throw new Error(w, e);
						}
					});
			System.err.println("put:" + (System.currentTimeMillis() - start));
			Assert.assertEquals(count, index.find(null, null).count());
			int depth = index.depth();
			System.err.println("depth:" + depth + " count:" + count);
			Assert.assertTrue(depth < count / 100);
			System.err.println("depth:" + (System.currentTimeMillis() - start));
		}
		try (PersistentBufferMap pers = new PersistentBufferMap(file, -1)) {
			MapIndex<String, String> index = new MapIndex<>(pers, "words",
					String.CASE_INSENSITIVE_ORDER, BBBroker.stringBroker, BBBroker.stringBroker);
			System.err.println("reopen:" + (System.currentTimeMillis() - start));

			index.find("A", "B").forEach(x -> System.err.println(x));
			// System.err.println("find1:" + (System.currentTimeMillis() -
			// start));
			Assert.assertEquals(Files.lines(Paths.get("/usr/share/dict/words"))
					.filter(w -> "B".equals(w) || w.startsWith("A")).count(),
					index.find("A", "B").count());
			System.err.println("find2:" + (System.currentTimeMillis() - start));

		}
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
