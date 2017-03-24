package net.soliddesign.map;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class WordIndexerTest {

	/** generally useful. put somewhere else: FIXME */
	static public ByteBuffer fromString(String s) {
		ByteBuffer bb = ByteBuffer.allocate(s.length() * 2);
		bb.asCharBuffer().put(s);
		return bb;
	}

	static public void main(String[] args) throws Exception {
		new File("words.db").delete();
		try (BufferedReader in = new BufferedReader(new FileReader("/home/joe/text"));
				PersistentBufferMap bm = new PersistentBufferMap(new File("words.db"), 100000)) {
			CloseableMap<String, Integer> map = new BufferMapAdapter<>(bm, "words", BBBroker.stringBroker,
					BBBroker.intBroker);
			Stream<String> m = in.lines()
					.flatMap(line -> Stream.of(line.split("\\W")));
			m.forEach(w -> {
				String word = w.toLowerCase();
				Integer count = map.get(word);
				if (count == null) {
					count = 1;
				} else {
					count++;
				}
				map.put(word, count);
			});
			map.remove("");
			map.entrySet().stream()
					.sorted((a, b) -> a.getValue() - b.getValue())
					.forEach(e -> System.err.println(e.getKey() + ": " + e.getValue()));
		}
	}

	/** generally useful. put somewhere else: FIXME */
	static public String toString(ByteBuffer bb) {
		return bb == null ? null : bb.asCharBuffer().toString();
	}
}
