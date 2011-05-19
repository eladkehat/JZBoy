/*
 * Copyright (c) 2011. Elad Kehat.
 * This software is provided under the MIT License:
 * http://www.opensource.org/licenses/mit-license.php
 */

package com.jzboy.couchdb.util;
import java.io.IOException;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;
import static org.junit.Assert.*;

public class JsonUtilsTest {

	@Test
	public void testSerializeJson() {
		ObjectNode root = new ObjectNode(JsonNodeFactory.instance);
		root.put("_id", "12345");
		root.put("_rev", "6-0cf82bfee87e7694d297f14373849401");
		root.put("text", "Some text.");
		ArrayNode list = root.putArray("list");
		for (int i=1; i<=5; i++)
			list.add(i);
		try {
			String str = JsonUtils.serializeJson(root);
			String expected = "{\"_id\":\"12345\",\"_rev\":\"6-0cf82bfee87e7694d297f14373849401\",";
			expected += "\"text\":\"Some text.\",\"list\":[1,2,3,4,5]}";
			assertEquals("serializeJson didn't serialize a string as expected", expected, str);
		} catch (IOException e) {
			fail(e.toString());
		}
	}

	@Test
	public void testSerializeError() {
		ObjectNode err = new ObjectNode(JsonNodeFactory.instance);
		err.put("error", "not_found");
		err.put("reason", "no_db_file");
		String str = JsonUtils.serializeError(err);
		String expected = "Error: not_found - no_db_file";
		assertEquals("serializeJson didn't serialize an error as expected", expected, str);
	}

	@Test
	public void testGetString() {
		ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
		node.put("field1", "value1");
		node.put("field2", 2);

		assertEquals("getString did not return a field's correct value",
                "value1", JsonUtils.getString(node, "field1", "default"));
		assertEquals("getString did not return the default supplied for a non-string value",
                "default", JsonUtils.getString(node, "field2", "default"));
		assertEquals("getString did not return the default supplied for a non-existent field",
                "default", JsonUtils.getString(node, "field3", "default"));
	}

	@Test
	public void testGetInt() {
		ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
		node.put("field1", 1);
		node.put("field2", "15");

		assertEquals("getInt did not return a field's correct value",
                1, JsonUtils.getInt(node, "field1", 15));
		assertEquals("getInt did not return the default supplied for a non-int value",
                2, JsonUtils.getInt(node, "field2", 2));
		assertEquals("getInt did not return the default supplied for a non-existent field",
                3, JsonUtils.getInt(node, "field3", 3));
	}

}
