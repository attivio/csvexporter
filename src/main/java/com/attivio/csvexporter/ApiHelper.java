package com.attivio.csvexporter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.w3c.dom.Document;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.client.IngestClient;
import com.attivio.sdk.client.SearchClient;
import com.attivio.sdk.geo.Point;
import com.attivio.sdk.geo.Shape;
import com.attivio.sdk.ingest.ContentPointer;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.ingest.IngestField;
import com.attivio.sdk.ingest.IngestFieldValue;
import com.attivio.sdk.search.SearchDocument;
import com.attivio.sdk.search.SearchField;
import com.attivio.sdk.search.SearchFieldValue;
import com.attivio.service.ServiceFactory;

/**
 * This class contains implementations that are no longer present in 5.5.0, but this project uses them
 * Note: I started using this somewhere towards the end of the first package.
 * 
 * @author dmitry.veber
 *
 */
public class ApiHelper {
	
	/**
	 * Retrieving the first string value from a field is an extremely frequent operation, and the new api has a danger of hitting an NPE
	 * This method handles the NPE, and allows for a relatively easy use of sed to replace all the old operations
	 * @param doc
	 * @param field
	 * @return
	 */
	public static String getFirstValueAsString(SearchDocument doc, String field){
		SearchFieldValue v = doc.getFirstValue(field);
		if (v == null) return null; else return v.stringValue();
	}
	
	/**
	 * Retrieving the first string value from a field is an extremely frequent operation, and the new api has a danger of hitting an NPE
	 * This method handles the NPE, and allows for a relatively easy use of sed to replace all the old operations
	 * @param doc
	 * @param field
	 * @return
	 */
	public static String getFirstValueAsString(IngestDocument doc, String field){
		IngestFieldValue v = doc.getFirstValue(field);
		if (v == null) return null; else return v.stringValue();
	}
	
	/**
	 * Getting the string equivalent of an MVF field is not in the api anymore, this method achieves the same thing
	 * @param host
	 * @return
	 * @throws AttivioException
	 */
	public static String getFieldStringValue(SearchField field, String sep){
		String result = "";
		for (SearchFieldValue value : field){
			result += sep + value.stringValue();
		}
		result = result.substring(sep.length()); // get rid of initial separator
		return result;
	}
	
	/**
	 * Search client is requested so frequently, that it's a good idea to make the api call in one place
	 * @param host
	 * @return
	 * @throws AttivioException
	 */
	public static SearchClient getSearchClient() throws AttivioException{
		return ServiceFactory.getService(SearchClient.class);
	}
	
	public static IngestClient getIngestClient() throws AttivioException{
		return ServiceFactory.getService(IngestClient.class);
	}
	
	public static IngestClient getIngestClient(String host) throws AttivioException{
		return ServiceFactory.getService(IngestClient.class, host);
	}
	
	/**
	 * Search client is requested so frequently, that it's a good idea to make the api call in one place
	 * @param host
	 * @return
	 * @throws AttivioException
	 */
	public static SearchClient getSearchClient(String host) throws AttivioException{
		return ServiceFactory.getService(SearchClient.class, host);
	}
	
	/**
	 * replaces the no longer present SearchDocument.getFieldValues(...) method
	 * @param doc
	 * @param field
	 * @return
	 */
	public static Collection<Object> getFieldValues(IngestField field){
		List<Object> list = new ArrayList<Object>();
		for (IngestFieldValue item : field){
			list.add(item.getValue());
		}
		return list;
	}
	
	/**
	 * replaces the no longer present SearchDocument.getFieldValues(...) method
	 * @param doc
	 * @param field
	 * @return
	 */
	public static Collection<Object> getFieldValues(SearchField field){
		List<Object> list = new ArrayList<Object>();
		for (SearchFieldValue item : field){
			list.add(item.getValue());
		}
		return list;
	}
	
	/**
	 * This method is intended to make it easy to add Object types to documents since the api no longer accepts Object types
	 * @param doc
	 * @param field
	 * @param value
	 */
	public static void addValueToField(IngestDocument doc, String field, Object value){
		
		if (value instanceof Boolean) doc.addValue(field, (Boolean)value);
    	else if (value instanceof ContentPointer) doc.addValue(field, (ContentPointer)value);
    	else if (value instanceof Date) doc.addValue(field, (Date)value);
    	else if (value instanceof Document) doc.addValue(field, (Document)value);
    	else if (value instanceof Number) doc.addValue(field, (Number)value);
    	else if (value instanceof Point) doc.addValue(field, (Point)value);
    	else if (value instanceof Shape) doc.addValue(field, (Shape)value);
    	else if (value instanceof String) doc.addValue(field, (String)value);
    	else if (value instanceof IngestFieldValue) addValueToField(doc, field, ((IngestFieldValue)value).getValue());
    	else if (value instanceof SearchFieldValue) addValueToField(doc, field, ((SearchFieldValue)value).getValue());
    	else throw new RuntimeException("Unrecognised data type for value being inserted into ["+field+"]");
	}
	
	/**
	 * This method is to replace addValues method, for documents, which was removed
	 * @param doc
	 * @param field
	 * @param values
	 */
	public static <T extends Object> void addValuesToField(IngestDocument doc, String field, List<T> values){
		for (Object value : values){
			addValueToField(doc, field, value);
		}
	}
	
	/**
	 * This method is to replace addValues method, for documents, which was removed
	 * @param doc
	 * @param field
	 * @param values
	 */
	public static <T extends Object> void addValuesToField(IngestDocument doc, String field, Collection<T> values){
		for (Object value : values){
			addValueToField(doc, field, value);
		}
	}
	
	/**
	 * This method is to replace addAll method on AttivioDocument, which was removed
	 * @param fromDoc
	 * @param toDoc
	 */
	public static void addAllFieldsFromDocToDoc(IngestDocument fromDoc, IngestDocument toDoc){
		for (IngestField field : fromDoc){
			addValuesToField(toDoc, field.getName(), getFieldValues(field));
		}
	}
	
	public static void addAllFieldsFromDocToDoc(SearchDocument fromDoc, IngestDocument toDoc){
		for (SearchField field : fromDoc){
			addValuesToField(toDoc, field.getName(), getFieldValues(field));
		}
	}
	
	/**
	 * This method is intended to make it easy to add Object types to fields since the api no longer accepts Object types
	 * @param field
	 * @param value
	 */
	public static void addValueToField(IngestField field, Object value){
		if (value instanceof Boolean) field.addValue((Boolean)value);
    	else if (value instanceof ContentPointer) field.addValue( (ContentPointer)value);
    	else if (value instanceof Date) field.addValue((Date)value);
    	else if (value instanceof Document) field.addValue((Document)value);
    	else if (value instanceof Number) field.addValue((Number)value);
    	else if (value instanceof Point) field.addValue((Point)value);
    	else if (value instanceof Shape) field.addValue((Shape)value);
    	else if (value instanceof String) field.addValue((String)value);
    	else if (value instanceof IngestFieldValue) addValueToField(field, ((IngestFieldValue)value).getValue());
    	else if (value instanceof SearchFieldValue) addValueToField(field, ((SearchFieldValue)value).getValue());
    	else throw new RuntimeException("Unrecognised data type for value being inserted into ["+field+"]");
	}

	

//	public static void addAllFieldValuesToDoc(IngestField fromField, String toField, IngestDocument toDoc) {
//		addValuesToField
//	}

}
