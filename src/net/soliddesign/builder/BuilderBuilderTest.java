package net.soliddesign.builder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.awt.Color;
import java.lang.reflect.UndeclaredThrowableException;

import org.junit.Test;

import net.soliddesign.builder.BuilderBuilder.Builder;
import net.soliddesign.builder.BuilderBuilder.Required;
import net.soliddesign.builder.BuilderBuilderTest.Person.BadPersonBuilder;
import net.soliddesign.builder.BuilderBuilderTest.Person.PersonBuilder;

public class BuilderBuilderTest {

	static public interface Person {
		/** broken Person Builder that allows setting a Person's alignment. */
		public interface BadPersonBuilder extends Builder<Person> {
			BadPersonBuilder alignment(String s);

			BadPersonBuilder finalField(boolean s);

			BadPersonBuilder invalidField(boolean s);

			BadPersonBuilder invalidProperty(boolean s);

			BadPersonBuilder name(String name);
		}

		/** example Builder */
		public interface PersonBuilder extends Builder<Person> {
			@Required
			PersonBuilder age(int age);

			PersonBuilder eyes(Color color);

			PersonBuilder height(double height);

			@Required
			PersonBuilder name(String name);
		}

		public int getAge();

		public Color getEyes();

		public double getHeight();

		public String getName();
	}

	/**
	 * This implementation tests that public setters work and that public
	 * instance variables work.
	 */
	static private class PersonImpl implements Person {
		// These two have setters.
		private String name;

		private int age;

		// These two are accessed as public fields.
		public double height;

		public Color eyes;

		// There is no setting, final field, which should fail.
		@SuppressWarnings("unused")
		final public boolean finalField = false;

		// There is no setting, only a private field, which should fail.
		@SuppressWarnings("unused")
		private boolean invalidField;

		@Override
		public int getAge() {
			return age;
		}

		@Override
		public Color getEyes() {
			return eyes;
		}

		@Override
		public double getHeight() {
			return height;
		}

		@Override
		public String getName() {
			return name;
		}

		@SuppressWarnings("unused")
		public void setAge(int age) {
			this.age = age;
		}

		// There is no setting, only a private property, which should fail.
		@SuppressWarnings("unused")
		private void setInvalidProperty(boolean bool) {
		}

		@SuppressWarnings("unused")
		public void setName(String name) {
			this.name = name;
		}
	}

	/**
	 * This is a lousy way to relate the builder to the business object class.
	 * It is for demonstration purposes only. It does allow for a private
	 * implementation though.
	 */
	static class Util {

		/** builder that should cause failure */
		public static BadPersonBuilder badBuilder() {
			return BuilderBuilder.builder(BadPersonBuilder.class, new PersonImpl());
		}

		/** builder that will build a Person using a PersonImpl */
		public static PersonBuilder builder() {
			return BuilderBuilder.builder(PersonBuilder.class, new PersonImpl());
		}
	}

	@Test(expected = IllegalStateException.class)
	public void bad() {
		// verify that the badBuilder mostly works
		Person joe = Util.badBuilder().name("Joe").build();
		assertEquals("Joe", joe.getName());
		assertTrue(joe instanceof Person);
		assertEquals(PersonImpl.class, joe.getClass());

		// should fail because you can't set a person's alignment.
		Util.badBuilder().name("Joe").alignment("evil").build();
	}

	@Test
	public void mapTest() {
		Person joe = BuilderBuilder.mapBuilder(Person.PersonBuilder.class, Person.class)
				.name("Joe")
				.age(45)
				.eyes(Color.blue)
				.build();
		assertEquals("Joe", joe.getName());
		assertEquals(0, joe.getAge());
		assertEquals(Color.blue, joe.getEyes());
		assertEquals(0.0, joe.getHeight(), 0);
		assertTrue(joe instanceof Person);
	}

	@Test
	public void simple() {
		Person joe = Util.builder()
				.name("Joe")
				.age(42)
				.eyes(Color.blue)
				.height(72)
				.build();
		assertEquals("Joe", joe.getName());
		assertEquals(42, joe.getAge());
		assertEquals(Color.blue, joe.getEyes());
		assertEquals(72, (int) joe.getHeight());
		assertTrue(joe instanceof Person);
		assertEquals(PersonImpl.class, joe.getClass());
	}

	@Test(expected = UndeclaredThrowableException.class)
	public void testFinalFieldAccess() {
		Util.badBuilder().finalField(true).build();
	}

	@Test(expected = IllegalStateException.class)
	public void testInvalidFieldAccess() {
		Util.badBuilder().invalidField(true).build();
	}

	@Test(expected = IllegalStateException.class)
	public void testInvalidPropertyAccess() {
		Util.badBuilder().invalidProperty(true).build();
	}

	@Test
	public void testRequired() {
		try {
			Util.builder().name("Joe").build();
		} catch (IllegalStateException e) {
			assertEquals("Required properties:[age]", e.getMessage());
		}

		try {
			Util.builder().build();
		} catch (IllegalStateException e) {
			// this is fragile. name may come before age if the data structure
			// is changed
			assertEquals("Required properties:[age, name]", e.getMessage());
		}
	}
}
