package net.soliddesign.builder;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** see BuilderBuilderTest for an example */
public class BuilderBuilder {
	public interface Builder<TYPE> {
		TYPE build();
	}

	/**
	 * Annotation used in Builder interface indicating a required field. build()
	 * will fail if all required fields are not set.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	public @interface Required {

	}

	private static final Map<Class<?>, Object> DEFAULT_VALUES = Stream
			.of(boolean.class, byte.class, char.class, double.class, float.class, int.class, long.class, short.class)
			.collect(Collectors.toMap(clazz -> (Class<?>) clazz, clazz -> Array.get(Array.newInstance(clazz, 1), 0)));

	@SuppressWarnings("unchecked")
	public static <TYPE, BUILDER_TYPE extends Builder<TYPE>, IMPL_TYPE extends TYPE> BUILDER_TYPE builder(
			final Class<BUILDER_TYPE> builderClass, final IMPL_TYPE objectImpl) {
		// because PropertyDescriptors can not be looked up individually as
		// needed,
		// we find all and index by name
		final Map<String, Method> setters = new HashMap<>();
		try {
			for (PropertyDescriptor pd : Introspector.getBeanInfo(objectImpl.getClass()).getPropertyDescriptors()) {
				setters.put(pd.getName(), pd.getWriteMethod());
			}
		} catch (IntrospectionException e) {
			throw new IllegalArgumentException("Unable to introspect on " + objectImpl.getClass().getName(), e);
		}

		// find required fields in builder
		final Set<String> requiredAttributes = new TreeSet<>();
		for (Method method : builderClass.getMethods()) {
			if (method.getAnnotation(Required.class) != null) {
				requiredAttributes.add(method.getName());
			}
		}

		return (BUILDER_TYPE) Proxy.newProxyInstance(builderClass.getClassLoader(), new Class<?>[] { builderClass },
				new InvocationHandler() {

					@Override
					public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
						if (method.equals(Builder.class.getMethod("build", new Class<?>[] {}))) {
							// verify all required fields are set
							if (!requiredAttributes.isEmpty()) {
								throw new IllegalStateException("Required properties:" + requiredAttributes);
							}
							return objectImpl;
						}
						String name = method.getName();
						Method writeMethod = setters.get(name);
						if (writeMethod == null) {
							try {
								objectImpl.getClass().getField(name).set(objectImpl, args[0]);
							} catch (NoSuchFieldException e) {
								throw new IllegalStateException(
										"Unknown Property " + name + " on a " + objectImpl.getClass().getName());
							}
						} else {
							writeMethod.invoke(objectImpl, args);
						}
						requiredAttributes.remove(name);
						return proxy;
					}
				});
	}

	@SuppressWarnings("unchecked")
	public static <TYPE, BUILDER_TYPE extends Builder<TYPE>, IMPL_TYPE extends TYPE> BUILDER_TYPE mapBuilder(
			final Class<BUILDER_TYPE> builderClass, final Class<TYPE> type) {
		// find required fields in builder
		final Set<String> requiredAttributes = new TreeSet<>();
		for (Method method : builderClass.getMethods()) {
			if (method.getAnnotation(Required.class) != null) {
				requiredAttributes.add(method.getName());
			}
		}
		return (BUILDER_TYPE) Proxy.newProxyInstance(builderClass.getClassLoader(), new Class<?>[] { builderClass },
				new InvocationHandler() {
					Map<String, Object> map = new HashMap<>();

					@Override
					public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
						if (method.equals(Builder.class.getMethod("build", new Class<?>[] {}))) {
							// verify all required fields are set
							if (!requiredAttributes.isEmpty()) {
								throw new IllegalStateException("Required properties:" + requiredAttributes);
							}
							return Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] { type },
									new InvocationHandler() {

										@Override
										public Object invoke(Object proxy, Method method, Object[] args)
												throws Throwable {
											String name = method.getName();
											if (name.startsWith("get")) {
												name = name.substring(3);
												name = name.substring(0, 1).toLowerCase() + name.substring(1);
											}
											if (method.getReturnType().isPrimitive()) {
												return DEFAULT_VALUES.get(method.getReturnType());
											}
											return map.get(name);
										}
									});
						}
						String name = method.getName();
						requiredAttributes.remove(name);
						map.put(name, args[0]);
						return proxy;
					}
				});
	}

}
