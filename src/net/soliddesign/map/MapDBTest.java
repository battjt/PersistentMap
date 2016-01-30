package net.soliddesign.map;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;

import org.junit.Test;

public class MapDBTest {
	@Test
	public void simple() throws Exception {
		File f = File.createTempFile("test.", ".mapdb");
		try (PersistentMap<String, String> map = PersistentMap.stringMap(f, 5)) {
			map.put("one", "red");
			map.put("two", "blue");
			map.put("three", "green");
		}
		try (PersistentMap<String, String> map = PersistentMap.stringMap(f, -1)) {
			assertEquals("red", map.get("one"));
			assertEquals("blue", map.get("two"));
			assertEquals("green", map.get("three"));
		} finally {
			f.delete();
		}
	}

	static class Person {
		Person() {
		}

		Person(String n, int a) {
			name = n;
			age = a;
		}

		String name;
		int age;
	}

	@Test
	public void gsonObjects() throws Exception {
		File f = File.createTempFile("test.", ".mapdb");
		try (PersistentMap<String, Person> map = PersistentMap.gsonMap(f, 5, String.class, Person.class)) {
			map.put("Joe", new Person("Joseph", 44));
		}
		try (PersistentMap<String, Person> map = PersistentMap.gsonMap(f, -1, String.class, Person.class)) {
			assertEquals(44, map.get("Joe").age);
		}
	}

	long i = 0;

	@Test
	public void lotsaObjects() throws Exception {
		File f = File.createTempFile("test.", ".mapdb");
		int count = (int) Files.lines(Paths.get("/usr/share/dict/words")).count();
		try (PersistentMap<String, Person> map = PersistentMap.gsonMap(f, count, String.class, Person.class)) {
			Files.lines(Paths.get("/usr/share/dict/words")).forEach(w -> map.put(w, new Person(w, w.length())));
		}
		try (PersistentMap<String, Person> map = PersistentMap.gsonMap(f, -1, String.class, Person.class)) {
			assertEquals(3, map.get("cat").age);
			assertEquals(5, map.get("money").age);
		}
	}
}
