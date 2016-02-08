import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;

public class TreeMapTest {
	public static void main(String[] args){
		TreeMap<Long, PriorityQueue<Long>> testMap = new TreeMap<Long, PriorityQueue<Long>>();
		
		
		PriorityQueue<Long> testQueue1 = new PriorityQueue<Long>();
		PriorityQueue<Long> testQueue2 = new PriorityQueue<Long>();
		PriorityQueue<Long> testQueue3 = new PriorityQueue<Long>();
		
		
		testQueue1.add(1L);
		testQueue1.add(4L);
		testQueue1.add(2L);
		testMap.put(3L, testQueue1);
		
		testQueue2.add(3L);
		testQueue2.add(1L);
		testQueue2.add(5L);
		testMap.put(5L, testQueue2);
		
		testQueue3.add(3L);
		testQueue3.add(1L);
		testQueue3.add(5L);
		testMap.put(1L, testQueue3);
		
		Iterator<Long> itr = testMap.descendingKeySet().iterator();
		while (itr.hasNext()) {
			long key = itr.next();
			System.out.println("Key:" + key);
			PriorityQueue<Long> printQueue = testMap.get(key);
			while (printQueue.size() > 0) {
				System.out.println(printQueue.poll());
//				System.out.println();
			}
		}
		
	}
	
}
