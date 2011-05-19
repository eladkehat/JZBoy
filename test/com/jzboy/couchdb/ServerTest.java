package com.jzboy.couchdb;

import java.util.ArrayList;
import org.codehaus.jackson.JsonNode;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * These tests run against a CouchDB instance running on localhost and listening on the default port (5984).
 * 
 * @author Elad Kehat
 */
public class ServerTest {

	static Server instance;

	@BeforeClass
	public static void setUpClass() throws Exception {
		instance = new Server();
	}

	@Test
	public void testVersion() throws Exception {
		String re = "^(\\d+\\.?)+$";
		String version = instance.version();
		assertTrue(version.matches(re));
	}

	@Test
	public void testAllDbs() throws Exception {
		final String dbName = "server-test-db" + System.currentTimeMillis();
		Database db = new Database(instance, dbName);
		db.create();
		try {
			ArrayList<String> result = instance.allDbs();
			assertTrue(result.contains(dbName));
		} finally {
			db.delete();
		}
	}

	@Test
	public void testConfig() throws Exception {
		JsonNode result = instance.config();
		// the result is a big JSON with CouchDB's configuration params
		// just check that one of them is indeed there
		String logLevel = result.get("log").get("level").getTextValue();
		assertNotNull(logLevel);
		assertFalse(logLevel.isEmpty());
	}

	@Test
	public void testStats() throws Exception {
		JsonNode result = instance.stats();
		// the result is a big JSON with CouchDB's statistics
		// just check that one of the expected parameters is there
		int currentOpenDbs = result.get("couchdb").get("open_databases").get("current").getIntValue();
		assertTrue(currentOpenDbs >= 0);
	}

	@Test
	public void testNextUUIDs() throws Exception {
		final int count = 10;
		ArrayList<String> result = instance.nextUUIDs(count);
		assertEquals(count, result.size());
		for (String uuid : result)
			assertNotNull(uuid);
	}

	@Test
	public void testNextUUID() throws Exception {
		String result = instance.nextUUID();
		assertNotNull(result);
	}

	@Test
	public void testActiveTasks() throws Exception {
		// Not much to test here really (unless we want to set up some length task to start running).
		// So I just call the method and ensure that it returns smoothly w/o any exceptions flying.
		ArrayList<JsonNode> result = instance.activeTasks();
		assertNotNull(result);
	}

	@Test
	public void testToString() {
		String expResult = "CouchDB @http://127.0.0.1:5984/";
		String result = instance.toString();
		assertEquals(expResult, result);
	}

}