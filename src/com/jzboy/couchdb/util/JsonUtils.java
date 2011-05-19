package com.jzboy.couchdb.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * JSON and Jackson-related utility methods
 * 
 * @author Elad Kehat
 */
public class JsonUtils {

	/**
	 * A simple {@link #JsonFactory JsonFactory} used by this class for serialization
	 */
	protected static final JsonFactory JsonFactory = new JsonFactory(new ObjectMapper());

	/**
	 * Serialize the JSON to a string using utf-8 encoding.
	 */
	public static String serializeJson(JsonNode json) throws IOException {
		JsonGenerator jsonGen;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		jsonGen = JsonFactory.createJsonGenerator(out, JsonEncoding.UTF8);
		jsonGen.writeTree(json);
		return out.toString("utf-8");
	}

	/**
	 * Utility method that creates a {@link org.codehaus.jackson.JsonParser} from the given string
	 */
	public static JsonParser createParser(String src) throws IOException {
		return JsonFactory.createJsonParser(src);
	}

	/**
	 * Serialize an error message from CouchDB, represented as a JSON.
	 * @param json	something like <pre>{"error":"not_found","reason":"no_db_file"}</pre>
	 * @return	something like <pre>Error: not_found - no_db_file</pre>
	 */
	public static String serializeError(JsonNode json) {
		JsonNode err = json.get("error");
		JsonNode reason = json.get("reason");
		StringBuilder sb = new StringBuilder();
		if (err != null)
			sb.append("Error: ").append(err.getTextValue());
		if (reason != null)
			sb.append(" - ").append(reason.getTextValue());
		return sb.toString();
	}
	/**
	 * When posting a Java utf-8 encoded JSON string to CouchDB 0.11 it complains that the string
	 * isn't in a valid utf-8 format. Re-encoding it to iso-8859-1 solves the problem.
	 * @param src	a string encoded in utf-8
	 * @return	same string, re-encoded to iso-8859-1
	 */
	public static String reencodeUtf8ToIso88591(String src) {
		try {
			return new String(src.getBytes("UTF-8"), "ISO-8859-1");
		} catch (UnsupportedEncodingException ex) {
			return src;
		}
	}

	/**
	 * If the specifried JSON contains a textual field fieldName, then its value is returned.
	 * Otherwise, returns the specified default value.
	 * Turns an ugly 4-liner boilerplate into a 1-liner.
	 */
	public static String getString(JsonNode json, String fieldName, String defaultValue) {
		JsonNode value = json.get(fieldName);
		if (value != null && value.isTextual())
			return value.getTextValue();
		return defaultValue;
	}

	/**
	 * If the specifried JSON contains an integer field fieldName, then its value is returned.
	 * Otherwise, returns the specified default value.
	 * Turns an ugly 4-liner boilerplate into a 1-liner (and I just broke comments DRY)
	 */
	public static int getInt(JsonNode json, String fieldName, int defaultValue) {
		JsonNode value = json.get(fieldName);
		if (value != null && value.isInt())
			return value.getIntValue();
		return defaultValue;
	}

}
