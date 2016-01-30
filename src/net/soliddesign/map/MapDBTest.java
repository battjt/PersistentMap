package net.soliddesign.map;

import java.io.File;

import org.junit.Test;

import static org.junit.Assert.*;

public class MapDBTest {
	@Test
	public void simple() throws Exception {
		File f = File.createTempFile("test.", ".mapdb");
		try (PersistentMap<String, String> map = PersistentMap.stringMap(f,5)) {
			map.put("one", "red");
			map.put("two", "blue");
			map.put("three", "green");
		}
		try (PersistentMap<String, String> map = PersistentMap.stringMap(f,-1)) {
			assertEquals("red", map.get("one"));
			assertEquals("blue", map.get("two"));
			assertEquals("green", map.get("three"));
		} finally {
			f.delete();
		}
	}
}
