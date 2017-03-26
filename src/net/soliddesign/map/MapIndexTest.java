package net.soliddesign.map;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class MapIndexTest {
	@Test
	public void lotsaObjects() throws Exception {
		File f = File.createTempFile("test.", ".mapdb");
		int count = (int) Files.lines(Paths.get("/usr/share/dict/words")).count();
		System.err.println("words found:" + count);
		long start = System.currentTimeMillis();

		// try (PersistentBufferMap pers = new PersistentBufferMap(f, count /
		// 4)) {
		{
			Map<ByteBuffer, ByteBuffer> pers = new HashMap<>();
			MapIndex<String, String> index = new MapIndex<>(pers, "words",
					String.CASE_INSENSITIVE_ORDER, BBBroker.stringBroker, BBBroker.stringBroker);
			Files.lines(Paths.get("/usr/share/dict/words"))
					// .limit(4000)
					.forEach(w -> {
						index.put(w, w);
					});
			// index.dump();
			index.find("A", "ASCII").forEach(x -> System.err.println("Boom: " + x));
		}

		try (PersistentBufferMap pers = new PersistentBufferMap(f, -1)) {
			MapIndex<String, String> index = new MapIndex<>(pers, "words",
					String.CASE_INSENSITIVE_ORDER, BBBroker.stringBroker, BBBroker.stringBroker);
			index.find("A", "B").forEach(x -> System.err.println(x));
		}
		Assert.assertTrue("Too slow.", System.currentTimeMillis() - start < 5000);
	}
}
