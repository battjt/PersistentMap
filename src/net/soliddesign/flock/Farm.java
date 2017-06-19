package net.soliddesign.flock;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import net.soliddesign.map.GsonMap;
import net.soliddesign.map.PersistentBufferMap;

@SuppressWarnings("restriction")
public class Farm {
	static public class Animal {
		public String id;
		public Animal sire, dame;
		public List<Event> event;
	}

	static public class Event {
		public List<Measure> measurements;
		public Date date;
		public String notes;
	}

	static public class Measure {
		public MeasureType type;
		public double value;
		public Unit unit;
	}

	enum MeasureType {
		WEIGHT, CONDITION
	}

	enum Unit {
		LBS, COUNT
	}

	static public void main(String[] a) throws Exception {
		PersistentBufferMap buffermap = new PersistentBufferMap(new File(a[0]), Integer.parseInt(a[1]));
		Map<String, Animal> farmMap = new GsonMap<>(buffermap, "farm", String.class, Farm.Animal.class);

		// pre-load test data
		{
			Animal joe = new Animal();
			Animal tom = new Animal();
			Animal dixie = new Animal();
			tom.id = "Tom";
			dixie.id = "Dixie";
			joe.id = "Joe";
			joe.dame = dixie;
			joe.sire = tom;
			joe.event = new ArrayList<>();
			Event e = new Event();
			e.date = new Date(1971, 11, 25);
			e.measurements = new ArrayList<>();
			Measure m = new Measure();
			m.type = MeasureType.WEIGHT;
			m.unit = Unit.LBS;
			m.value = 9.5;
			e.measurements.add(m);
			e.notes = "Stuff happened.";
			joe.event.add(e);
			farmMap.put(joe.id, joe);
			farmMap.put(tom.id, tom);
			farmMap.put(dixie.id, dixie);
		}

		HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
		server.createContext("/list", e -> {
			e.sendResponseHeaders(200, 0);
			OutputStream o = e.getResponseBody();
			try (PrintWriter w = new PrintWriter(new OutputStreamWriter(o))) {
				w.print("[");
				farmMap.values()
						.forEach(animal -> w.println("'" + animal.id + "',"));
				w.print("]");
			}
		});
		server.createContext("/test", e -> {
			e.sendResponseHeaders(200, 0);
			OutputStream o = e.getResponseBody();
			try (PrintWriter w = new PrintWriter(new OutputStreamWriter(o))) {
				w.print("test");
			}
			e.getResponseBody().close();
		});
		server.createContext("/animal/get", e -> {
			String str = e.getRequestURI().toString();
			str = str.replaceFirst(".*/([^/?]+).*", "$1");
			System.err.println(str + ":" + new Gson().toJson(farmMap.get(str)));
			send(e, farmMap.get(str));
		});
		server.createContext("/animal/put", e -> {
			Animal animal = read(e, Animal.class);
			farmMap.put(animal.id, animal);
		});
		server.createContext("/", e -> {
			File file = new File(e.getRequestURI().toString().substring(1));
			e.sendResponseHeaders(200, file.length());
			Files.copy(file.toPath(), e.getResponseBody());
			e.getResponseBody().close();
		});
		server.setExecutor(null); // creates a default executor
		server.start();
	};

	private static <T> T read(HttpExchange e, Class<T> cls) {
		return new Gson().fromJson(new InputStreamReader(e.getRequestBody()), cls);
	}

	private static void send(HttpExchange e, Object x) throws IOException {
		e.sendResponseHeaders(200, 0);
		Gson g = new Gson();
		try (JsonWriter w = new JsonWriter(new OutputStreamWriter(e.getResponseBody()))) {
			g.toJson(g.toJsonTree(x), w);
		}
	}
}