package org.mpstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class BTreeStore implements MPStore {

	private int order = 5;

	class BTData {
		BTData(Serializable data) {
			this.data = data;
			stored = false;
		}
		Serializable data;
		long fileLoc;
		boolean stored;
	}

	class BTNode {
		BTNode parent;
		BTNode children[];
		Comparable keys[];
		BTData values[];
		boolean leaf;
		int n;
		BTNode() {
			children = new BTNode[2 * order];
			keys = new Comparable[2 * order - 1];
			values = new BTData[2*order-1];
			parent = null;
			leaf = true;
			n = 0;
		}

		void splitChild(int point) {
			BTNode y = children[point];
			if ( y.n == order * 2 - 1 ) {
				//System.err.println( "Splitting" );
				BTNode z = new BTNode();	
				z.leaf = y.leaf;
				z.n = order - 1;
				for ( int i = 0; i < order - 1; i++ ) {
					z.keys[i] = y.keys[ i + order ];
					z.values[i] = y.values[ i + order ];
				}
				if ( !leaf ) {
					for ( int i = 0; i < order; i++ ) {
						z.children[i] = y.children[ order + i ];
					}
				}
				y.n = order - 1;
				for ( int i = n + 1; i > point + 1; i-- ) {
					children[i] = children[i-1];
				}
				z.parent = this;
				children[ point + 1 ] = z;
				for ( int i = n; i > point; i-- ) {
					keys[i] = keys[i-1];
					values[i] = values[i-1];
				}
				keys[point] = y.keys[order-1];
				values[point] = y.values[order-1];
				n++;
				//System.err.println(this);
				//System.err.println(z);
				//System.err.println(y);
				write(this);
				write(z);
				write(y);
			}			
		}

		public void insertNode(Comparable newKey, Serializable value) {
			int i = n - 1;
			if ( leaf ) {
				while ( i >= 0 && newKey.compareTo(keys[i]) < 0 ) {
					keys[i+1] = keys[i];
					values[i+1] = values[i];
					i--;
				}
				keys[i+1] = newKey;
				values[i+1] = new BTData(value);
				n++;
				write(this);
			} else {
				while ( i >= 0 && newKey.compareTo(keys[i]) < 0 ) {
					i--;
				}
				i++;
				read( children[i] );
				if ( children[i].n == 2 * order - 1  ) {
					splitChild(i);
					if ( newKey.compareTo(keys[i]) < 0 ) {
						i++;
					}
				}
				children[i].insertNode( newKey, value );
			}
		}

		public void treeCollect(Comparable searchKey, List out) {
			int i = 0; 
			while ( i < n && searchKey.compareTo(keys[i]) > 0 ) {
				i++;
			}
			while ( i < n && searchKey.equals(keys[i]) ) {
				out.add( values[i].data );
				if ( !leaf ) {
					read(children[i]);
					children[i].treeCollect(searchKey, out );
				}
				i++;
			}
			if ( !leaf ) {
				read(children[i]);
				children[i].treeCollect(searchKey, out);
			}		
		}

		public Serializable getFirst(Comparable searchKey) {
			int i = 0; 
			while ( i < n && searchKey.compareTo(keys[i]) > 0 ) {
				i++;
			}
			if ( i < n && searchKey.equals(keys[i]) ) {
				return values[i].data;
			}
			if ( !leaf ) {
				read(children[i]);
				return children[i].getFirst(searchKey);
			}			
			return null;
		}

		void explore(String prefix) {
			if (!leaf) {
				for ( int i = 0; i <= n; i++) {
					children[i].explore( prefix + "\t");
					if ( i<n ) {
						System.err.println( prefix + Integer.toString(i) + ":" + keys[i] + "(" + values[i].data + ")" );
					}
				}
			} else {
				for ( int i = 0; i < n; i++) {
					System.err.println( prefix + Integer.toString(i) + ":" + keys[i] + "(" + values[i].data + ")" );
				}
			}

		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			boolean first = true;
			for ( int i = 0; i < n; i++) {
				if ( first ) {
					first = false;
				} else {
					sb.append(",");
				}
				sb.append( keys[i] );
				sb.append( "(" + values[i].data.toString() + ")" );
			}
			return sb.toString();
		}

	}


	RandomAccessFile dataFile, indexFile;
	BTNode root;

	public BTreeStore(File fileBase) throws FileNotFoundException {
		root = allocateNode();
		root.leaf = true;
		root.n = 0;
		write(root);
		//dataFile = new RandomAccessFile(fileBase, "rw" );
		//indexFile = new RandomAccessFile( new File( fileBase.getAbsolutePath() + ".idx"), "rw");
	}	

	void explore() {
		root.explore("");
		System.err.println("----");
	}

	private void write(BTNode node) {
		//do something here
	}

	private void read(BTNode node) {

	}

	public void add(Comparable key, Serializable data) {
		BTNode r = root;
		if ( r.n == 2 * order -1 ) {
			BTNode s = allocateNode();
			s.leaf = false;
			root = s;
			s.n = 0;
			s.children[0] = r;
			r.parent = s;
			//explore();
			s.splitChild(0);
			//explore();
			s.insertNode( key, data );
			//explore();
		} else {
			r.insertNode(key, data);
		}
	}

	public Iterable<Serializable> get(Comparable key) {
		LinkedList<Serializable>  outList = new LinkedList<Serializable>();
		root.treeCollect(key, outList );
		return outList;
	}

	public class KeyIterator implements Iterable<Comparable>, Iterator<Comparable> {
		BTNode curNode;
		Comparable outKey;
		LinkedList<Integer> pointStack;
		int curPoint;
		KeyIterator( ) {
			curNode = null;
			pointStack = new LinkedList<Integer>();
			curNode = root;
			while ( !curNode.leaf ) {
				curNode = curNode.children[0];
				pointStack.add(0);
			}
			curPoint = 0;				
		}

		public Iterator<Comparable> iterator() {			
			return this;
		}

		public boolean hasNext() {
			outKey = null;
			if ( curPoint < curNode.n ) {
				outKey = curNode.keys[ curPoint ];
				if ( !curNode.leaf ) {
					curNode = curNode.children[ curPoint ];
					pointStack.add(curPoint);
					curPoint = 0;
				} else {
					curPoint++;
				}
			} else {
				if ( curNode.parent != null ) {
					curPoint = pointStack.removeLast();					
					curNode = curNode.parent;
					outKey = curNode.keys[ curPoint ];
				}
			}
			
			if ( outKey != null ) 
				return true;
			return false;
			
		}

		public Comparable next() {
			return outKey;
		}

		public void remove() { }	
	}

	public Iterable<Comparable> listKeys() {
		return new KeyIterator();
	}


	BTNode allocateNode() {
		return new BTNode();
	}


	public static void main(String [] args) throws FileNotFoundException {
		BTreeStore bt = new BTreeStore(new File("test_data"));


		for ( int i = 0; i < 100; i++ ) {
		//	bt.add( UUID.randomUUID().toString(), UUID.randomUUID().toString() );
		}

		bt.add("test_a", "data_a_1");
		bt.add("test_a", "data_a_2");
		bt.add("test_b", "data_b_1");
		bt.add("test_c", "data_c_1");
		bt.add("test_a", "data_a_3");
		bt.add("test_d", "data_d_1");
		bt.add("test_d", "data_d_2");
		bt.add("test_d", "data_d_3");
		//bt.explore();
		bt.add("test_d", "data_d_4");
		//bt.explore();
		bt.add("test_b", "data_b_2");
		bt.add("test_e", "data_e_1");
		bt.add("test_e", "data_e_2");


		//for ( int i = 0; i < 100; i++ ) {
		//	bt.add( UUID.randomUUID().toString(), UUID.randomUUID().toString() );
		//}		

		for (Comparable key : bt.listKeys() ) {
			System.out.println( key );
		}
		
		/*
		for ( Serializable key : bt.get("test_a") ) {
			System.out.println( key );
		}

		for ( Serializable key : bt.get("test_b") ) {
			System.out.println( key );
		}

		for ( Serializable key : bt.get("test_c") ) {
			System.out.println( key );
		}

		for ( Serializable key : bt.get("test_d") ) {
			System.out.println( key );
		}
		 */
		//bt.explore();
	}

}
