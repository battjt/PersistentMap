package net.soliddesign.map;

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
		long id; // do we really want to store this in teh value also?
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
	}

	/**
	 * a tree mapping of value of T to list of K
	 */
	private class TreeNode {
		long id; // do we really want to store this in teh value also?
		long left;
		long list;
		long right;
		T value;

		void dump(String prefix) {
			System.err.println(prefix + value);
			prefix += "  ";
			if (left != 0) {
				trees.get(left).dump(prefix);
			}
			if (right != 0) {
				trees.get(right).dump(prefix);
			}
		}

		// FIXME this should recurse on demand, not at call time.
		public Stream<K> find(T min, T max) {
			int cMin = comparator.compare(min, value);
			int cMax = comparator.compare(value, max);

			Stream<K> stream = Stream.empty();
			if (cMin < 0 && left != 0) {
				stream = trees.get(left).find(min, max);
			}
			if (cMin <= 0 && cMax <= 0) {
				stream = Stream.concat(stream, items.get(list).list());
			}
			if (cMax < 0 && right != 0) {
				stream = Stream.concat(stream, trees.get(right).find(min, max));
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
				trees.put(id, this);
				return 0;
			} else if (c > 0) {
				if (right == 0) {
					right = newTree(0, 0, value, newList(0, key).id).id;
					trees.put(id, this);
					return 1;
				} else {
					double put = trees.get(right).put(value, key) / 2;
					if (put > Math.random()) {
						rotateLeft();
					}
					return put;
				}
			} else {
				if (left == 0) {
					left = newTree(0, 0, value, newList(0, key).id).id;
					trees.put(id, this);
					return 1;
				} else {
					double put = trees.get(left).put(value, key) / 2;
					if (put > Math.random()) {
						rotateRight();
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
					trees.put(id, this);
					items.remove(item.id);
				} else {
					item.remove(k);
				}
			} else if (c < 0 && left != 0) {
				trees.get(left).remove(v, k);
			} else if (right != 0) {
				trees.get(right).remove(v, k);
			}
		}

		// FIXME, not atomic
		private void rotateLeft() {
			MapIndex<T, K>.TreeNode old = trees.get(right);
			right = old.left;
			old.left = old.id;
			old.id = id;

			id = old.left;
			trees.put(id, this);
			trees.put(old.id, old);
		}

		// FIXME, not atomic
		private void rotateRight() {
			MapIndex<T, K>.TreeNode old = trees.get(left);
			left = old.right;
			old.right = id;
			old.id = id;

			id = old.right;
			trees.put(id, this);
			trees.put(old.id, old);
		}

	}

	static private Random random = new Random();

	/**
	 * just guess until we find an unused ID. Should be fine since we are using
	 * longs for ids
	 */
	static private long newMapId(Map<Long, ?> map) {
		long id;
		// only positives. FIXME why?
		while (map.containsKey(id = Math.abs(random.nextLong()))) {
			;
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

	public void dump() {
		trees.get(0L).dump("");
	}

	public Stream<K> find(T min, T max) {
		MapIndex<T, K>.TreeNode root = trees.get(0L);
		return root == null ? Stream.empty() : root.find(min, max);
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
		tree.id = newMapId(MapIndex.this.trees);
		tree.left = left;
		tree.right = right;
		tree.value = value;
		tree.list = list;
		trees.put(tree.id, tree);
		return tree;
	}

	public void put(T v, K k) {
		MapIndex<T, K>.TreeNode root = trees.get(0L);
		if (root == null) {
			root = new TreeNode();
			root.list = newList(0, k).id;
			root.value = v;
			trees.put(0L, root);
		} else {
			root.put(v, k);
		}
	}

	public void remove(T v, K k) {
		MapIndex<T, K>.TreeNode root = trees.get(0L);
		if (root != null) {
			root.remove(v, k);
		}
	}

	@Override
	public String toString() {
		return "trees:" + trees + "\nitems:" + items;
	}

}
