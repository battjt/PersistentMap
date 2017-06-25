package net.soliddesign.map;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

		public Spliterator<K> spliterator() {
			return new Spliterator<K>() {
				long n = id;

				@Override
				public int characteristics() {
					return CONCURRENT;
				}

				@Override
				public long estimateSize() {
					return Long.MAX_VALUE;
				}

				@Override
				public boolean tryAdvance(Consumer<? super K> action) {
					if (n > 0) {
						MapIndex<T, K>.ListItem item = items.get(n);
						n = item.next;
						action.accept(item.value);
						return true;
					}
					return false;
				}

				@Override
				public Spliterator<K> trySplit() {
					return null;
				}
			};
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

		public int depth() {
			int depth = 0;
			depth = right > 0 ? Math.max(depth, getNode(right).depth()) : depth;
			depth = left > 0 ? Math.max(depth, getNode(left).depth()) : depth;
			depth++;
			return depth;
		}

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

		public Spliterator<K> find(T min, T max) {
			return new Spliterator<K>() {
				{
					List<Supplier<Spliterator<K>>> subs = new ArrayList<>();
					int cMin = min == null ? -1 : comparator.compare(min, value);
					int cMax = max == null ? -1 : comparator.compare(value, max);
					if (cMin < 0 && left != 0) {
						subs.add(() -> getNode(left).find(min, max));
					}
					if (cMin <= 0 && cMax <= 0) {
						subs.add(() -> items.get(list).spliterator());
					}
					if (cMax < 0 && right != 0) {
						subs.add(() -> getNode(right).find(min, max));
					}
					i = subs.iterator();
				}
				Spliterator<K> s;
				Iterator<Supplier<Spliterator<K>>> i;

				@Override
				public int characteristics() {
					return ORDERED | CONCURRENT | IMMUTABLE;
				}

				@Override
				public long estimateSize() {
					return Long.MAX_VALUE;
				}

				@Override
				public boolean tryAdvance(Consumer<? super K> action) {
					if (s == null && i.hasNext()) { // initial call
						s = i.next().get();
					}
					do {
						if (s.tryAdvance(action)) {
							return true;
						}
						s = i.hasNext() ? i.next().get() : null;
					} while (s != null);
					return false;
				}

				@Override
				public Spliterator<K> trySplit() {
					return i.hasNext() ? i.next().get() : null;
				}
			};
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
						rotateLeft();
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
			// System.err.println("rotateLeft:" + depth());
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
			// System.err.println("rotateRight:" + depth());
			MapIndex<T, K>.TreeNode old = getNode(left);
			left = old.right;
			old.right = old.id;

			// swap IDs so that outside references are correct
			old.id = id;
			id = old.right;

			putNode(this);
			putNode(old);
		}

		public long size() {
			long size = 1;
			if (right > 0) {
				size += getNode(right).size();
			}
			if (left > 0) {
				size += getNode(left).size();
			}
			return size;
		}

		@Override
		public String toString() {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			dump("", new PrintStream(out));
			return out.toString();
		}

	}

	static private Random random = new Random();

	/**
	 * just guess until we find an unused ID. Should be fine since we are using
	 * longs for ids
	 */
	static private long newMapId(Map<Long, ?> map) {
		long id = 1 + Math.abs(random.nextLong());
		while (map.containsKey(id)) {
			id++;
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

	public int depth() {
		return getNode(0).depth();
	}

	public String depthReport() {
		MapIndex<T, K>.TreeNode root = getNode(0);
		int r = root.right == 0 ? 0 : getNode(root.right).depth();
		int l = root.left == 0 ? 0 : getNode(root.left).depth();
		long rs = root.right == 0 ? 0 : getNode(root.right).size();
		long ls = root.left == 0 ? 0 : getNode(root.left).size();
		String str = "S:" + root.size() + " R:" + r + " RS:" + rs + " L:" + l + " LS:" + ls;
		// ByteArrayOutputStream out = new ByteArrayOutputStream();
		// dump(new PrintStream(out));

		return str;
	}

	public void dump(PrintStream out) {
		MapIndex<T, K>.TreeNode node = trees.get(0L);
		if (node != null) {
			node.dump("", out);
		}
	}

	public Stream<K> find(T min, T max) {
		MapIndex<T, K>.TreeNode root = getNode(0L);
		return root == null ? Stream.empty() : StreamSupport.stream(root.find(min, max), false);
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
			synchronized (this) {
				root = trees.get(0L);
				if (root == null) {
					root = new TreeNode();
					root.list = newList(0, k).id;
					root.value = v;
					putNode(root);
				}
			}
		} else {
			root.put(v, k);
		}
	}

	private void putNode(MapIndex<T, K>.TreeNode node) {
		trees.put(node.id, node);
	}

	public void remove(T v, K k) {
		MapIndex<T, K>.TreeNode root = getNode(0L);
		if (root != null) {
			root.remove(v, k);
		}
	}

	@Override
	public String toString() {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		dump(new PrintStream(out));
		return out.toString();
		// return "lists:" + items.toString() + "\ntrees:" + trees.toString();
	}
}
