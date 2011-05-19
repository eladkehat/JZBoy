package com.jzboy.couchdb.util;
import java.io.IOException;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Elad Kehat
 */
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
			assertEquals(expected, str);
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
		assertEquals(expected, str);
	}

	@Test
	public void testGetString() {
		ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
		node.put("field1", "value1");
		node.put("field2", 2);

		assertEquals("value1", JsonUtils.getString(node, "field1", "default"));
		assertEquals("default", JsonUtils.getString(node, "field2", "default"));
		assertEquals("default", JsonUtils.getString(node, "field3", "default"));
	}

	@Test
	public void testGetInt() {
		ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
		node.put("field1", 1);
		node.put("field2", "15");

		assertEquals(1, JsonUtils.getInt(node, "field1", 15));
		assertEquals(2, JsonUtils.getInt(node, "field2", 2));
		assertEquals(3, JsonUtils.getInt(node, "field3", 3));
	}

}
