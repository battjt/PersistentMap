# PersistentMap
java.util.Map that is persistent to a disk based hashtable.

The underelying key and value types are ByteBuffers.  Serialization methods are declared at initialization.  Gson serializers and String to String maps are provided.  There is no garbage collection, so periodically, maps need manually garbage collected (create a new one with the old data).

	public void stringExample() throws Exception {
		File f = File.createTempFile("test.", ".mapdb");
		// create new map
		try (StringMap map = StringMap.create(new PersistentBufferMap(f, 3))) {
			map.put("one", "red");
			map.put("two", "blue");
			map.put("three", "green");
		}
		// open existing map
		try (StringMap map = StringMap.create(new PersistentBufferMap(f, -1))) {
			assertEquals("red", map.get("one"));
			assertEquals("blue", map.get("two"));
			assertEquals("green", map.get("three"));
			assertNull(map.get("four"));
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
	
	public void gsonObjects() throws Exception {
		File f = File.createTempFile("test.", ".mapdb");
		try (GsonMap<String, Person> map = GsonMap.create(new PersistentBufferMap(f, 5), String.class, Person.class)) {
			map.put("Joe", new Person("Joseph", 44));
		}
		try (GsonMap<String, Person> map = GsonMap.create(new PersistentBufferMap(f, -1), String.class, Person.class)) {
			assertEquals(44, map.get("Joe").age);
			assertEquals("Joseph", map.get("Joe").name);
		}
	}
