package demo.infinispan;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class NoSQLManagerTest {

	private NoSQLManager<String, String> manager;

	@Before
	public void setup() {
		this.manager = new NoSQLManager<String, String>();
	}
	
	@Test
	public void test() {
		Map<String, String> map = generateDumyRecords(getMap().size());
		manager.save(map);
		generateDumyRecords(map.size());
		manager.saveButRollback(map);
		assertEquals(1000, manager.count());
	}

	private Map<String, String> generateDumyRecords(int size) {
		Map<String, String> map = getMap();
		for (int i = size; i <= size+999; i++) {
			map.put("key"+i, "value"+i);	
		}
		return map;
	}

	private Map<String, String> getMap() {
		Map<String, String> map = new HashMap<String, String>();
		return map;
	}

}
