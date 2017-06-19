package net.soliddesign.map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class PersistentBufferMapTest {
	static public class H {
		public String str;

		H(String s) {
			str = s;
		}

		@Override
		public boolean equals(Object o) {
			return str.equals(o);
		}

		@Override
		public int hashCode() {
			return 5;
		}

		@Override
		public String toString() {
			return "H(" + str + ")";
		}
	}

	public static class Person {
		public final String name;

		public final int age;

		Person(String n, int a) {
			name = n;
			age = a;
		}
	}

	private static final String LOREM_IPSUM = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer ultricies elementum lacinia. Nam euismod viverra ultrices. Sed fringilla, ipsum a volutpat aliquam, tortor leo tristique diam, in consequat purus neque vitae enim. Mauris et efficitur lorem. Duis in fringilla dui. In lacinia dictum nulla pharetra efficitur. Suspendisse ac lacus nulla. Aenean egestas, orci ac dictum semper, sapien ligula tincidunt tortor, sit amet ullamcorper leo orci eu libero. Quisque in aliquet diam. Praesent aliquam nullam.";

	@Test
	public void bucketTest() throws Exception {
		File f = File.createTempFile("test.", ".mapdb");
		try (PersistentBufferMap pers = new PersistentBufferMap(f, 7)) {
			BufferMapAdapter<Long, Long> longMap = new BufferMapAdapter<>(new PersistentBufferMap(f, -1), null,
					BBBroker.longBroker,
					BBBroker.longBroker);
			Random rand = new Random();
			for (int i = 0; i < 100; i++) {
				long id = 0;
				while (longMap.containsKey(id)) {
					id = rand.nextLong();
				}
				Assert.assertEquals(i, pers.size());
				// Assert.assertEquals(i, pers.keySet().size());
				// Assert.assertEquals(i, pers.values().size());
				longMap.put(id, -id);
			}
			System.err.println(
					new BufferMapAdapter<>(new PersistentBufferMap(f, -1), null, BBBroker.longBroker,
							BBBroker.longBroker));
		}
	}

	@Test
	public void gsonObjects() throws Exception {
		File f = File.createTempFile("test.", ".mapdb");
		try {
			try (GsonMap<String, Person> map = new GsonMap<>(new PersistentBufferMap(f, 5), "gsonObjects",
					String.class, Person.class)) {
				map.put("Joe", new Person("Joseph", 44));
			}
			try (GsonMap<String, Person> map = new GsonMap<>(new PersistentBufferMap(f, -1), "gsonObjects",
					String.class, Person.class)) {
				assertEquals(44, map.get("Joe").age);
				assertEquals("Joseph", map.get("Joe").name);
			}
		} finally {
			f.delete();
		}
	}

	@Test
	/**
	 * Load /usr/dict/words into a String->Person map where each
	 * person.name=word and person.age=word.length.
	 *
	 * Tests insertion speed, reopening a DB, and removal.
	 */
	public void lotsaObjects() throws Exception {
		File f = File.createTempFile("test.", ".mapdb");
		int count = (int) Files.lines(Paths.get("/usr/share/dict/words")).count();
		System.err.println("words found:" + count);
		long start = System.currentTimeMillis();
		try (GsonMap<String, Person> map = new GsonMap<>(new PersistentBufferMap(f, count), "words", String.class,
				Person.class)) {
			Files.lines(Paths.get("/usr/share/dict/words")).forEach(w -> map.put(w, new Person(w, w.length())));
		}
		try (GsonMap<String, Person> map = new GsonMap<>(new PersistentBufferMap(f, -1), "words", String.class,
				Person.class)) {
			assertEquals(3, map.get("cat").age);
			assertEquals(5, map.get("money").age);
			Assert.assertTrue(map.size() == count);
			map.remove("money");
			Assert.assertTrue(map.size() == count - 1);
		}
		try (GsonMap<String, Person> map = new GsonMap<>(new PersistentBufferMap(f, -1), "words", String.class,
				Person.class)) {
			Assert.assertTrue(map.size() == count - 1);
		}
		Assert.assertTrue("Too slow.", System.currentTimeMillis() - start < 5000);
	}

	@Test
	public void simple() throws Exception {
		File f = File.createTempFile("test.", ".mapdb");
		try {
			PersistentBufferMap map2 = new PersistentBufferMap(f, 3);
			// Map<ByteBuffer, ByteBuffer> map2 = new HashMap<>();
			try (StringMap map = new StringMap(map2, "simple")) {
				map.put("four", "apple");
				String apple = map.put("four", "grape");
				assertEquals("apple", apple);
				String grape = map.put("four", "mellon");
				assertEquals("grape", grape);
				assertEquals("apple", apple);
				map.put("one", "red");
				map.put("two", "blue");
				map.put("three", "green");
				// verify content
				assertEquals("red", map.get("one"));
				assertEquals("red", map.get("one"));
				assertEquals("blue", map.get("two"));
				assertEquals("green", map.get("three"));
				assertEquals("mellon", map.get("four"));
				assertNull(map.get("five"));

				// verify key and value collections that are generated by
				// map.stream()
				List<String> keys = Arrays.asList("one", "two", "three", "four");
				List<String> values = Arrays.asList("red", "blue", "green", "mellon");
				assertEquals(new HashSet<>(keys), map.keySet());
				assertEquals(new HashSet<>(values), map.values());

				// verify hashcode and equals
				HashMap<String, String> m = new HashMap<>();
				for (int i = 0; i < keys.size(); i++) {
					m.put(keys.get(i), values.get(i));
				}
				assertEquals(m.hashCode(), map.hashCode());
				assertEquals(m, map);
				m.put("zero", "nothing");
				Assert.assertNotEquals(m.hashCode(), map.hashCode());
				Assert.assertNotEquals(m, map);

				// test giant keys
				map.put(LOREM_IPSUM, "Lorem Ipsum");
				assertEquals("Lorem Ipsum", map.get(LOREM_IPSUM));
				assertNull(map.get(LOREM_IPSUM + "a"));
			}
			try (StringMap map = new StringMap(new PersistentBufferMap(f, -1), "simple")) {
				assertEquals("red", map.get("one"));
				assertEquals("blue", map.get("two"));
				assertEquals("green", map.get("three"));
				assertEquals("mellon", map.remove("four"));
				assertNull(map.get("four"));

				// test giant keys
				assertEquals("Lorem Ipsum", map.get(LOREM_IPSUM));
				assertNull(map.get(LOREM_IPSUM + "a"));
				assertEquals("Lorem Ipsum", map.remove(LOREM_IPSUM));
				Assert.assertFalse(map.containsKey(LOREM_IPSUM));
			}
			try (StringMap map = new StringMap(new PersistentBufferMap(f, -1), "simple")) {
				assertNull(map.get("four"));
				// System.err.println(new Gson().toJson(map));
				assertEquals(new Gson().fromJson("{'one':'red','two':'blue','three':'green'}",
						new TypeToken<Map<String, String>>() {
						}.getType()), map);
			}
		} finally {
			f.delete();
		}

	}

	@Test
	public void testCollisions() throws Exception {
		File f = File.createTempFile("test.", ".mapdb");
		try (GsonMap<H, Person> map = new GsonMap<>(new PersistentBufferMap(f, 5), "testCollisions",
				H.class, Person.class)) {
			map.put(new H("Joe"), new Person("Joseph", 45));
			System.out.println("t1:" + map);
			Assert.assertEquals(45, map.get(new H("Joe")).age);
			map.put(new H("Missy"), new Person("Missy", 44));
			System.out.println("t2:" + map);
			Assert.assertEquals(45, map.get(new H("Joe")).age);
			Assert.assertEquals(44, map.get(new H("Missy")).age);
			map.put(new H("Meeks"), new Person("Meeks", 43));
			map.put(new H("Mike"), new Person("Mike", 43));
			Assert.assertEquals(4, map.size());
			Assert.assertEquals(4, map.keySet().size());
		}
		try (GsonMap<H, Person> map = new GsonMap<>(new PersistentBufferMap(f, -1), "testCollisions",
				H.class, Person.class)) {
			map.keySet().forEach(k -> System.err.println("key:" + k));
			Assert.assertEquals(4, map.size());
			Assert.assertEquals(4, map.keySet().size());
		}
	}

}
