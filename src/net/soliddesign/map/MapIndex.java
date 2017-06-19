package net.soliddesign.map;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Stream;

/**
 *
 * @param <T>
 *            Type of the value used in the index
 * @param <K>
 *            Type of the key of the objects being indexed
 */
public class MapIndex<T, K> {
	private class ListItem {
		long id; // do we really want to store this in the value also?
		long next;
		K value;

		private Stream<K> list() {
			if (next == 0) {
				return Stream.of(value);
			}
			return Stream.concat(Stream.of(value), items.get(next).list());
		}

		public void remove(K k) {
			if (next != 0) {
				ListItem item = items.get(next);
				if (item.value.equals(k)) {
					next = item.next;
					items.remove(item.id);
					items.put(id, this);
				} else {
					item.remove(k);
				}
			}
		}

		@Override
		public String toString() {
			String str = id + ":" + value.toString();
			if (next != 0) {
				str += "," + items.get(next).toString();
			}
			return str;
		}
	}

	/**
	 * a tree mapping of value of T to list of K
	 */
	private class TreeNode {
		long id; // do we really want to store this in the value also?
		long left;
		long list;
		long right;
		T value;

		void dump(String prefix, PrintStream out) {
			out.println(prefix + " ID:" + id + " v:" + value + " list:" + items.get(list));
			if (left != 0) {
				MapIndex<T, K>.TreeNode node = getNode(left);
				node.dump(prefix + " L:" + left + " ", out);
			}
			if (right != 0) {
				MapIndex<T, K>.TreeNode node = getNode(right);
				node.dump(prefix + " R:" + right + " ", out);
			}
		}

		// FIXME this should recurse on demand, not at call time.
		public Stream<K> find(T min, T max) {
			int cMin = min == null ? -1 : comparator.compare(min, value);
			int cMax = max == null ? -1 : comparator.compare(value, max);
			boolean useLeft = cMin < 0 && left != 0;
			boolean useThis = cMin <= 0 && cMax <= 0;
			boolean useRight = cMax < 0 && right != 0;

			Stream<K> stream = Stream.empty();
			if (useLeft) {
				stream = getNode(left).find(min, max);
			}
			if (useThis) {
				stream = Stream.concat(stream, items.get(list).list());
			}
			if (useRight) {
				stream = Stream.concat(stream, getNode(right).find(min, max));
			}
			return stream;
		}

		/**
		 * @return 1/depth. zero means that there was no insertion, so there is
		 *         no need for any rotation.
		 */
		double put(T value, K key) {
			int c = comparator.compare(value, this.value);
			if (c == 0) {
				this.list = newList(list, key).id;
				putNode(this);
				return 0;
			} else if (c > 0) {
				if (right == 0) {
					right = newTree(0, 0, value, newList(0, key).id).id;
					putNode(this);
					return 1;
				} else {
					double put = getNode(right).put(value, key) / 2;
					if (put > Math.random()) {
						// rotateLeft();
					}
					return put;
				}
			} else {
				if (left == 0) {
					left = newTree(0, 0, value, newList(0, key).id).id;
					putNode(this);
					return 1;
				} else {
					double put = getNode(left).put(value, key) / 2;
					if (put > Math.random()) {
						// rotateRight();
					}
					return put;
				}
			}
		}

		/** will leave treenodes, but will remove keys from lists */
		public void remove(T v, K k) {
			int c = comparator.compare(v, this.value);
			if (c == 0) {
				ListItem item = items.get(list);
				if (item.value.equals(k)) {
					list = item.next;
					putNode(this);
					items.remove(item.id);
				} else {
					item.remove(k);
				}
			} else if (c < 0 && left != 0) {
				getNode(left).remove(v, k);
			} else if (right != 0) {
				getNode(right).remove(v, k);
			}
		}

		// FIXME, not atomic
		private void rotateLeft() {
			MapIndex<T, K>.TreeNode old = getNode(right);
			right = old.left;
			old.left = old.id;
			// swap IDs so that outside references are correct
			old.id = id;
			id = old.left;

			putNode(this);
			putNode(old);
		}

		// FIXME, not atomic
		private void rotateRight() {
			MapIndex<T, K>.TreeNode old = getNode(left);
			left = old.right;
			old.right = old.id;

			// swap IDs so that outside references are correct
			old.id = id;
			id = old.right;

			putNode(this);
			putNode(old);
		}

		@Override
		public String toString() {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			dump("", new PrintStream(out));
			return out.toString();
		}

	}

	static private Random random = new Random();

	private static long id = 1;

	/**
	 * just guess until we find an unused ID. Should be fine since we are using
	 * longs for ids
	 */
	static private long newMapId(Map<Long, ?> map) {
		// long id = 1 + Math.abs(random.nextLong());
		while (map.containsKey(++id)) {
			System.err.println("collision:" + id++);
		}
		return id;
	}

	final private Comparator<T> comparator;

	/** storage of list items in persistent buffer map */
	final private Map<Long, ListItem> items;

	/** storage of tree nodes in persistent buffer map */
	final private Map<Long, TreeNode> trees;

	/**
	 *
	 * @param pers
	 *            backing store
	 * @param name
	 *            index name
	 * @param comparator
	 *            index sort
	 * @param keyBroker
	 *            broker for key to real objects
	 * @param valueBroker
	 *            broker for value being indexed
	 */
	public MapIndex(Map<ByteBuffer, ByteBuffer> pers, String name, Comparator<T> comparator, BBBroker<K> keyBroker,
			BBBroker<T> valueBroker) {
		this.comparator = comparator;
		this.items = new BufferMapAdapter<>(pers, name + ":items", BBBroker.longBroker, new BBBroker<ListItem>() {

			@Override
			public Optional<MapIndex<T, K>.ListItem> fromBB(ByteBuffer bb) {
				MapIndex<T, K>.ListItem item = new ListItem();
				item.next = longBroker.fromBB(bb).get();
				item.value = keyBroker.fromBB(bb).get();
				item.id = longBroker.fromBB(bb).get();
				return Optional.of(item);
			}

			@Override
			public ByteBuffer toBB(MapIndex<T, K>.ListItem v) {
				return BBBroker.toBB(longBroker.toBB(v.next),
						keyBroker.toBB(v.value),
						longBroker.toBB(v.id));
			}
		});
		trees = new BufferMapAdapter<>(pers, name + ":tree", BBBroker.longBroker, new BBBroker<TreeNode>() {

			@Override
			public Optional<MapIndex<T, K>.TreeNode> fromBB(ByteBuffer bb) {
				TreeNode tree = new TreeNode();
				tree.id = longBroker.fromBB(bb).get();
				tree.left = longBroker.fromBB(bb).get();
				tree.right = longBroker.fromBB(bb).get();
				tree.value = valueBroker.fromBB(bb).get();
				tree.list = longBroker.fromBB(bb).get();
				return Optional.of(tree);
			}

			@Override
			public ByteBuffer toBB(MapIndex<T, K>.TreeNode v) {
				return BBBroker.toBB(longBroker.toBB(v.id),
						longBroker.toBB(v.left),
						longBroker.toBB(v.right),
						valueBroker.toBB(v.value),
						longBroker.toBB(v.list));
			}
		});
	}

	public void dump(PrintStream out) {
		MapIndex<T, K>.TreeNode node = trees.get(0L);
		if (node != null) {
			node.dump("", out);
		}
	}

	public Stream<K> find(T min, T max) {
		MapIndex<T, K>.TreeNode root = getNode(0L);
		return root == null ? Stream.empty() : root.find(min, max);
	}

	private MapIndex<T, K>.TreeNode getNode(long ind) {
		MapIndex<T, K>.TreeNode node = trees.get(ind);
		if (node == null) {
			throw new NullPointerException("node missing:" + ind);
		}
		return node;
	}

	private ListItem newList(long next, K key) {
		ListItem item = new ListItem();
		item.id = newMapId(items);
		item.value = key;
		item.next = next;
		items.put(item.id, item);
		return item;
	}

	private TreeNode newTree(long left, long right, T value, long list) {
		TreeNode tree = new TreeNode();
		tree.id = newMapId(trees);
		tree.left = left;
		tree.right = right;
		tree.value = value;
		tree.list = list;
		putNode(tree);
		return tree;
	}

	public void put(T v, K k) {
		MapIndex<T, K>.TreeNode root = trees.get(0L);
		if (root == null) {
			root = new TreeNode();
			root.list = newList(0, k).id;
			root.value = v;
			putNode(root);
		} else {
			root.put(v, k);
		}
	}

	private void putNode(MapIndex<T, K>.TreeNode node) {
		MapIndex<T, K>.TreeNode oldNode = trees.put(node.id, node);
		if (oldNode != null) {
			// System.err.println("Replaced " + oldNode + " with " + node);
		}
	}

	public void remove(T v, K k) {
		MapIndex<T, K>.TreeNode root = getNode(0L);
		if (root != null) {
			root.remove(v, k);
		}
	}

	@Override
	public String toString() {
		// ByteArrayOutputStream out = new ByteArrayOutputStream();
		// dump(new PrintStream(out));
		// return out.toString();
		return "lists:" + items.toString() + "\ntrees:" + trees.toString();
	}
}
