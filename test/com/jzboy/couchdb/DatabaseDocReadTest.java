package com.jzboy.couchdb;

import com.jzboy.couchdb.util.JsonUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Tests for the Database class methods that retrieve documents from the database.
 * These tests run against a CouchDB instance running on localhost and listening on the default port (5984).
 * 
 * @author Elad Kehat
 */
public class DatabaseDocReadTest {

	static String dbName;
	static Database instance;
	// initial documents - maps of uuids to the doc json
	static final String designDocName = "d";
	static final String viewName = "v";
	static final Map<String, String> docs = new HashMap<String, String>() {{
		put("1", "{\"field1\":\"value1\",\"show\":true}");
		put("2", "{\"field1\":\"value2\",\"show\":false}");
		put("3", "{\"field1\":\"value3\",\"show\":true}");
		put("_design/"+designDocName,
				"{\"views\":{\"" + viewName + "\":{\"map\":\"function(doc) { if(doc.show) emit(doc._id, null);}\"}}}");
	}};

    @BeforeClass
    public static void setUpClass() throws Exception {
		dbName = "server-test-db" + System.currentTimeMillis();
		instance = new Database(dbName);
		instance.create();
		// create some documents
		for (Map.Entry<String, String> doc : docs.entrySet()) {
			instance.createDocument(doc.getKey(), doc.getValue(), false);
		}
    }

	@AfterClass
	public static void tearDownClass() throws Exception {
		instance.delete();
	}

	@Test
	public void testGetDocument() throws Exception {
		final String id = "1";
		String str = docs.get(id);
		JsonNode exp = JsonUtils.createParser(str).readValueAsTree();
		Document doc = instance.getDocument(id);
		assertEquals(exp, doc.getJson());
	}

	@Test
	public void testGetDocumentOrNull() throws Exception {
		final String existingId = "1";
		String str = docs.get(existingId);
		JsonNode exp = JsonUtils.createParser(str).readValueAsTree();
		Document doc = instance.getDocumentOrNull(existingId);
		assertEquals(exp, doc.getJson());

		final String missingId = "no-such-id";
		doc = instance.getDocumentOrNull(missingId);
		assertNull(doc);

		// ensure we still get an exception on errors other than 404
		Database wrong = new Database("NO-SUCH-DATABASE"); // only lowercase letters are allowed by couchdb
		try {
			wrong.getDocumentOrNull(missingId);
			fail("Expected an exception when calling getDocumentOrNull() on a db with an illegal name");
		} catch (CouchDBException ex) {
			assertTrue(ex.getStatusCode() != 404);
		}
	}

	@Test
	public void testChanges() throws Exception {
		final int since = 2;
		List<NameValuePair> params = new ArrayList<NameValuePair>() {{
			add(new BasicNameValuePair("since", Integer.toString(since)));
		}};
		ArrayList<JsonNode> changes = instance.changes(params);
		int expSize = docs.size() - since;
		assertTrue(changes.size() >= expSize);
		JsonNode change = changes.get(0);
		assertEquals(3, JsonUtils.getInt(change, "seq", 0));
	}

	@Test
	public void testGetDocumentsIncludeDocs() throws Exception {
		ArrayList<Document> res = instance.getDocuments(docs.keySet(), true);
		assertEquals(docs.size(), res.size());
		for (Document doc : res) {
			String str = docs.get(doc.getId());
			JsonNode exp = JsonUtils.createParser(str).readValueAsTree();
			assertEquals(exp, doc.getJson());
		}
	}

	@Test
	public void testGetDocumentsExcludeDocs() throws Exception {
		ArrayList<Document> res = instance.getDocuments(docs.keySet(), false);
		assertEquals(docs.size(), res.size());
		for (Document doc : res) {
			assertNotNull(docs.get(doc.getId()));
			assertEquals("{}", doc.getJson().toString());
		}
	}

	@Test
	public void testGetDocumentsWithParams() throws Exception {
		final int skip = 2;
		List<NameValuePair> params = new ArrayList<NameValuePair>() {{
			add(new BasicNameValuePair("skip", Integer.toString(skip)));
		}};
		ArrayList<Document> res = instance.getDocuments(docs.keySet(), params);
		int expSize = docs.size() - skip;
		assertEquals(expSize, res.size());
	}
	
	@Test
	public void testGetAllDocumentsIncludeDocs() throws Exception {
		ArrayList<Document> res = instance.getAllDocuments(true);
		assertEquals(docs.size(), res.size());
		for (Document doc : res) {
			String str = docs.get(doc.getId());
			JsonNode exp = JsonUtils.createParser(str).readValueAsTree();
			assertEquals(exp, doc.getJson());
		}
	}

	@Test
	public void testGetAllDocumentsExcludeDocs() throws Exception {
		ArrayList<Document> res = instance.getAllDocuments(false);
		assertEquals(docs.size(), res.size());
		for (Document doc : res) {
			assertNotNull(docs.get(doc.getId()));
			assertEquals("{}", doc.getJson().toString());
		}
	}

	@Test
	public void testGetAllDocumentsWithParams() throws Exception {
		final String key = (String) docs.keySet().toArray()[0];
		List<NameValuePair> params = new ArrayList<NameValuePair>() {{
			add(new BasicNameValuePair("startkey", "\"" + key + "\""));
			add(new BasicNameValuePair("endkey", "\"" + key + "\""));
			add(new BasicNameValuePair("include_docs", "true"));
		}};
		ArrayList<Document> res = instance.getAllDocuments(params);
		assertEquals(1, res.size());
		String str = docs.get(key);
		JsonNode exp = JsonUtils.createParser(str).readValueAsTree();
		assertEquals(exp, res.get(0).getJson());
	}

	@Test
	public void testDesignDocumentInfo() throws Exception {
		JsonNode result = instance.designDocumentInfo(designDocName);
		String resName = result.get("name").getTextValue();
		assertEquals(designDocName, resName);
	}

	@Test
	public void testQueryView() throws Exception {
		List<NameValuePair> params = new ArrayList<NameValuePair>() {{
			add(new BasicNameValuePair("include_docs", "true"));
		}};
		ArrayList<Document> results = instance.queryView(designDocName, viewName, params);
		ArrayList<String> expDocIds = new ArrayList<String>();
		for (Map.Entry<String, String> entry : docs.entrySet()) {
			if (entry.getValue().contains("\"show\":true")) {
				expDocIds.add(entry.getKey());
			}
		}
		assertEquals(expDocIds.size(), results.size());
		for (Document doc : results) {
			assertTrue(expDocIds.contains(doc.getId()));
			assertTrue(doc.hasKey());
			String str = docs.get(doc.getId());
			JsonNode exp = JsonUtils.createParser(str).readValueAsTree();
			assertEquals(exp, doc.getJson());
		}
	}

	@Test
	public void testQueryViewRaw() throws Exception {
		List<NameValuePair> params = new ArrayList<NameValuePair>() {{
			add(new BasicNameValuePair("include_docs", "true"));
		}};
		JsonNode result = instance.queryViewRaw(designDocName, viewName, params);
		int expCount = 0;
		for (Map.Entry<String, String> entry : docs.entrySet()) {
			if (entry.getValue().contains("\"show\":true")) {
				expCount++;
			}
		}
		assertEquals(expCount, result.get("total_rows").getIntValue());
	}

	@Test
	public void testGetFromView() throws Exception {
		List<NameValuePair> params = new ArrayList<NameValuePair>() {{
			add(new BasicNameValuePair("offset", "0"));
			add(new BasicNameValuePair("include_docs", "true"));
		}};
		ArrayList<String> allViewDocIds = new ArrayList<String>();
		for (Map.Entry<String, String> entry : docs.entrySet()) {
			if (entry.getValue().contains("\"show\":true")) {
				allViewDocIds.add(entry.getKey());
			}
		}
		List<String> keys = allViewDocIds.subList(0, 1);
		ArrayList<Document> results = instance.getFromView(designDocName, viewName, keys, params);
		assertEquals(keys.size(), results.size());
		for (Document doc : results) {
			assertTrue(keys.contains(doc.getId()));
			String str = docs.get(doc.getId());
			JsonNode exp = JsonUtils.createParser(str).readValueAsTree();
			assertEquals(exp, doc.getJson());
		}
	}

	@Test
	public void testTempViewString() throws Exception {
		String jsonStr = "{\"map\":\"function(doc) { if (doc.field1=='value1') { emit(null, doc.field1); } }\"}";
		ArrayList<Document> results = instance.tempView(jsonStr);
		assertEquals(1, results.size());
		Document doc = results.get(0);
		assertEquals("1", doc.getId());
		assertEquals("value1", doc.getJson().getValueAsText());
	}

	@Test
	public void testTempViewJson() throws Exception {
		ObjectNode viewJson = new ObjectNode(JsonNodeFactory.instance);
		viewJson.put("map", "function(doc) { if (doc.field1=='value1') { emit(null, doc.field1); } }");
		ArrayList<Document> results = instance.tempView(viewJson);
		assertEquals(1, results.size());
		Document doc = results.get(0);
		assertEquals("1", doc.getId());
		assertEquals("value1", doc.getJson().getValueAsText());
	}

	@Test
	public void testCompactViews() throws Exception {
		try {
			instance.compactViews(designDocName);
		} catch (CouchDBException e) {
			fail(e.toString());
		}
	}

	@Test
	public void testCleanupViews() throws Exception {
		try {
			instance.cleanupViews();
		} catch (CouchDBException e) {
			fail(e.toString());
		}
	}

}