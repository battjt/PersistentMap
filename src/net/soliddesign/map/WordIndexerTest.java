package net.soliddesign.map;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class WordIndexerTest {
	static public void main(String[] args) throws Exception {
		new File("words.db").delete();
		try (BufferedReader in = new BufferedReader(new FileReader("/home/joe/text"));
				PersistentBufferMap bm = new PersistentBufferMap(new File("words.db"), 100000)) {
			CloseableMap<String, Integer> map = bm.adapt(s -> {
				ByteBuffer bb = ByteBuffer.allocate(s.length() * 2);
				bb.asCharBuffer().put(s);
				return bb;
			} , b -> b == null ? null : b.asCharBuffer().toString(), i -> ByteBuffer.allocate(Integer.BYTES).putInt(i),
					b -> b == null ? null : b.getInt());
			in.lines().flatMap(line -> Stream.of(line.split("\\W"))).forEach(word -> {
				word = word.toLowerCase();
				Integer count = map.get(word);
				if (count == null) {
					count = 1;
				} else {
					count++;
				}
				map.put(word, count);
			});
			map.remove("");
			map.entrySet().stream().sorted((a, b) -> a.getValue() - b.getValue())
					.forEach(e -> System.err.println(e.getKey() + ": " + e.getValue()));
		}
	}
}
