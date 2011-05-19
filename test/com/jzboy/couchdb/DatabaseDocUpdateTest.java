/*
 * Copyright (c) 2011. Elad Kehat.
 * This software is provided under the MIT License:
 * http://www.opensource.org/licenses/mit-license.php
 */

package com.jzboy.couchdb;

import com.jzboy.couchdb.util.JsonUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.zip.DeflaterOutputStream;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Tests for the Database class methods that modify documents in the database.
 * These tests run against a CouchDB instance running on localhost and listening on the default port (5984).
 */
public class DatabaseDocUpdateTest {

	static String dbName;
	static Database instance;
	static byte[] attachment;

    @BeforeClass
    public static void setUpClass() throws Exception {
		dbName = "server-test-db" + System.currentTimeMillis();
		instance = new Database(dbName);
		instance.create();

		// create a gzipped attachment
		final String inputString = "blahblahblah??";
		byte[] input = inputString.getBytes("utf-8");
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		DeflaterOutputStream deflater = new DeflaterOutputStream(os);
		deflater.write(input, 0, input.length);
		deflater.close();
		attachment = os.toByteArray();
    }

	@AfterClass
	public static void tearDownClass() throws Exception {
		instance.delete();
	}

	@Test
	public void testCreateDocumentFromJsonStrWithId() throws Exception {
		final String uuid = instance.getServer().nextUUID();
		final String jsonStr = "{\"field1\":\"value1\",\"field2\":2}";
		Document result = instance.createDocument(uuid, jsonStr, false);
		assertEquals(uuid, result.getId());
		assertTrue(result.hasRev());
		assertTrue(result.getJson().get("ok").getBooleanValue());
		Document doc = instance.getDocument(uuid);
		assertEquals(jsonStr, JsonUtils.serializeJson(doc.getJson()));
	}

	@Test
	public void testCreateDocumentFromJsonNodeWithId() throws Exception {
		final String uuid = instance.getServer().nextUUID();
		ObjectNode docJson = new ObjectNode(JsonNodeFactory.instance);
		docJson.put("field1", "value1");
		docJson.put("field2", 2);
		Document result = instance.createDocument(uuid, docJson, false);
		assertEquals(uuid, result.getId());
		assertTrue(result.hasRev());
		assertTrue(result.getJson().get("ok").getBooleanValue());
	}

	@Test
	public void testCreateDocumentInBatch() throws Exception {
		final String uuid = instance.getServer().nextUUID();
		final String jsonStr = "{\"field1\":\"value1\",\"field2\":2}";
		Document result = instance.createDocument(uuid, jsonStr, true);
		assertEquals(uuid, result.getId());
		assertTrue(result.getJson().get("ok").getBooleanValue());
		// no revision returned in batch mode
		assertFalse(result.hasRev());
		// wait for 2 seconds, then try to load this document
		Thread.sleep(2000);
		Document doc = instance.getDocument(uuid);
		assertEquals(jsonStr, JsonUtils.serializeJson(doc.getJson()));
	}

	@Test
	public void testCreateDocumentFromDocument() throws Exception {
		final String uuid = instance.getServer().nextUUID();
		ObjectNode docJson = new ObjectNode(JsonNodeFactory.instance);
		docJson.put("field1", "value11");
		docJson.put("field2", 22);
		Document newDoc = new Document(uuid, docJson);
		Document result = instance.createDocument(newDoc, false);
		assertEquals(uuid, result.getId());
		assertTrue(result.hasRev());
		assertTrue(result.getJson().get("ok").getBooleanValue());

		Document doc = instance.getDocument(uuid);
		assertEquals(docJson, doc.getJson());
	}

	@Test
	public void testUpdateDocument() throws Exception {
		final String uuid = instance.getServer().nextUUID();
		final String jsonStr = "{\"field1\":\"value135\",\"field2\":246}";
		// create a new document
		Document result = instance.createDocument(uuid, jsonStr, false);
		assertTrue(result.getJson().get("ok").getBooleanValue());
		// check the value of 'field1'
		Document doc = instance.getDocument(uuid);
		assertEquals("value135", JsonUtils.getString(doc.getJson(), "field1", null));
		final String rev1 = doc.getRev();
		// update 'field1' to a different value
		ObjectNode node = (ObjectNode) doc.getJson();
		node.put("field1", "value99");
		Document upDoc = instance.updateDocument(doc);
		// ensure that we got a higher revision number
		assertTrue(upDoc.getRev().compareTo(rev1) > 0);
		// check the value of 'field1'
		Document docAgain = instance.getDocument(uuid);
		assertEquals("value99", JsonUtils.getString(docAgain.getJson(), "field1", null));
	}

	@Test
	public void testDeleteDocument() throws Exception {
		final String uuid = instance.getServer().nextUUID();
		final String jsonStr = "{\"field1\":\"value135\",\"field2\":246}";
		// create a new document
		Document result = instance.createDocument(uuid, jsonStr, false);
		assertTrue(result.getJson().get("ok").getBooleanValue());
		// ensure that document exists
		Document doc = instance.getDocument(uuid);
		assertEquals(jsonStr, JsonUtils.serializeJson(doc.getJson()));
		// now delete it
		String rev = instance.deleteDocument(doc);
		// the delete stub revision should be higher than the original doc revision
		assertTrue(doc.getRev().compareTo(rev) < 0);
		// ensure that it doesn't exist anymore
		try {
			instance.getDocument(uuid);
			fail("Document was supposed to have been deleted");
		} catch (CouchDBException ex) {
			assertEquals(404, ex.getStatusCode());
		}
	}

	private byte[] readInputStream(InputStream is) throws IOException {
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		int i;
		while ((i = is.read()) > -1)
			os.write(i);
		return os.toByteArray();
	}

	@Test
	public void testSaveAndGetAttachment() throws Exception {
		final String fileName = "blahblah.txt.gz";
		final String uuid = instance.getServer().nextUUID();
		final String jsonStr = "{\"field1\":\"value135\",\"field2\":246}";
		final String contentType = "application/x-gzip";

		Document doc = new Document(uuid, null, jsonStr);
		Document resDoc = instance.saveAttachment(doc, fileName, attachment, contentType);
		assertTrue(resDoc.getJson().get("ok").getBooleanValue());
		assertEquals(uuid, resDoc.getId());

		// now get the stored attachment in its raw form
		HttpResponse httpRes = instance.getAttachmentRaw(uuid, fileName);
		assertEquals(200, httpRes.getStatusLine().getStatusCode());
		HttpEntity ent = httpRes.getEntity();
		assertEquals(contentType, ent.getContentType().getValue());
		byte[] resAttachment = readInputStream(ent.getContent());
		assertArrayEquals(attachment, resAttachment);

		// get the stored attachment as an input stream
		resAttachment = instance.getAttachment(uuid, fileName);
		assertArrayEquals(attachment, resAttachment);
	}


	@Test
	public void testDeleteAttachment() throws Exception {
		// save an attachment
		final String fileName = "blahblah.txt.gz";
		final String uuid = instance.getServer().nextUUID();
		final String jsonStr = "{\"field1\":\"value135\",\"field2\":246}";
		final String contentType = "application/x-gzip";
		Document doc = new Document(uuid, null, jsonStr);
		Document resDoc = instance.saveAttachment(doc, fileName, attachment, contentType);
		assertTrue(resDoc.getJson().get("ok").getBooleanValue());
		assertEquals(uuid, resDoc.getId());
		// ensure it's retrievable
		byte[] resAttachment = instance.getAttachment(uuid, fileName);
		assertTrue(resAttachment.length > 0);
		// delete it
		Document del = instance.deleteAttachment(resDoc, fileName); // use the doc that has a correct revision id
		assertEquals(uuid, del.getId());
		assertTrue(del.getRev().compareTo(resDoc.getRev()) > 0);
		// ensure that it's gone
		try {
			resAttachment = instance.getAttachment(uuid, fileName);
			fail("Attachment wasn't deleted");
		} catch (CouchDBException e) {
			assertEquals(404, e.getStatusCode());
		}
	}

	@Test
	public void testNumDocsPendingBulkUpdate() throws Exception {
		ArrayList<String> uuids = instance.getServer().nextUUIDs(2);
		final String json1 = "{\"field1\":\"value1\",\"field2\":2}";
		final String json2 = "{\"field1\":\"value2\",\"field2\":3}";
		Document doc1 = new Document(uuids.get(0), null, json1);
		Document doc2 = new Document(uuids.get(1), null, json2);
		instance.saveInBulk(doc1);
		instance.saveInBulk(doc2);
		int pending = instance.numDocsPendingBulkUpdate();
		assertEquals(2, pending);
		instance.clearBulkUpdatesCache();
		pending = instance.numDocsPendingBulkUpdate();
		assertEquals(0, pending);
	}

	@Test
	public void testSaveInBulk() throws Exception {
		instance.setBulkUpdatesLimit(2);
		ArrayList<String> uuids = instance.getServer().nextUUIDs(2);
		final String json1 = "{\"field1\":\"value1\",\"field2\":2}";
		final String json2 = "{\"field1\":\"value2\",\"field2\":3}";
		Document doc1 = new Document(uuids.get(0), null, json1);
		Document doc2 = new Document(uuids.get(1), null, json2);
		assertEquals(0, instance.numDocsPendingBulkUpdate());
		instance.saveInBulk(doc1);
		assertEquals(1, instance.numDocsPendingBulkUpdate());
		instance.saveInBulk(doc2);
		assertEquals(0, instance.numDocsPendingBulkUpdate());

		Document resDoc1 = instance.getDocument(uuids.get(0));
		assertEquals(json1, JsonUtils.serializeJson(resDoc1.getJson()));
		Document resDoc2 = instance.getDocument(uuids.get(1));
		assertEquals(json2, JsonUtils.serializeJson(resDoc2.getJson()));
	}

	@Test
	public void testSaveInBulkWithoutContent() throws Exception {
		Document doc = new Document("1", "1-012345"); // id and rev, no JSON
		try {
			instance.saveInBulk(doc);
			fail("Document with no content was allowed to be added to bulk cache");
		} catch (IllegalArgumentException e) { }
	}

	@Test
	public void testDeleteInBulk() throws Exception {
		instance.setBulkUpdatesLimit(2);
		ArrayList<String> uuids = instance.getServer().nextUUIDs(2);
		final String json1 = "{\"field1\":\"value15\",\"field2\":15}";
		final String json2 = "{\"field1\":\"value25\",\"field2\":25}";
		Document saved1 = instance.createDocument(uuids.get(0), json1, false);
		Document saved2 = instance.createDocument(uuids.get(1), json2, false);
		saved1.setJson(null);
		saved2.setJson(null);
		instance.deleteInBulk(saved1);
		instance.deleteInBulk(saved2);

		assertEquals(0, instance.numDocsPendingBulkUpdate());
		try {
			instance.getDocument(uuids.get(0));
			fail("Document wasn't deleted");
		} catch (CouchDBException ex1) {
			assertEquals(404, ex1.getStatusCode());
		}
		try {
			instance.getDocument(uuids.get(1));
			fail("Document wasn't deleted");
		} catch (CouchDBException ex2) {
			assertEquals(404, ex2.getStatusCode());
		}
	}

	@Test
	public void testFlushBulkUpdatesCache() throws Exception {
		instance.setBulkUpdatesLimit(1000);
		ArrayList<String> uuids = instance.getServer().nextUUIDs(2);
		final String json1 = "{\"field1\":\"value1\",\"field2\":2}";
		final String json2 = "{\"field1\":\"value2\",\"field2\":3}";
		Document doc1 = new Document(uuids.get(0), null, json1);
		Document doc2 = new Document(uuids.get(1), null, json2);
		assertEquals(0, instance.numDocsPendingBulkUpdate());
		instance.saveInBulk(doc1);
		instance.saveInBulk(doc2);
		assertEquals(2, instance.numDocsPendingBulkUpdate());

		ArrayList<JsonNode> report = instance.flushBulkUpdatesCache(false, true);
		assertEquals(0, instance.numDocsPendingBulkUpdate());
		for (JsonNode resDoc : report) {
			assertTrue(uuids.contains(resDoc.get("id").getTextValue()));
		}
	}

}