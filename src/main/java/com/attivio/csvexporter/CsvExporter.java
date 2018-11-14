package com.attivio.csvexporter;

import java.io.File;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.client.SearchClient;
import com.attivio.sdk.client.streaming.StreamingQueryResponse;
import com.attivio.sdk.connector.DocumentPublisher;
import com.attivio.sdk.error.ConnectorError;
import com.attivio.sdk.error.PlatformError;
import com.attivio.sdk.scanner.DataSourceScanner;
import com.attivio.sdk.schema.FieldNames;
import com.attivio.sdk.search.JoinRollupMode;
import com.attivio.sdk.search.QueryLanguages;
import com.attivio.sdk.search.QueryRequest;
import com.attivio.sdk.search.QueryResponse;
import com.attivio.sdk.search.SearchDocument;
import com.attivio.sdk.search.StreamingQueryRequest;
import com.attivio.sdk.search.StreamingQueryRequest.DocumentStreamingMode;
import com.attivio.sdk.search.facet.FacetBucket;
import com.attivio.sdk.search.facet.FacetRequest;
import com.attivio.sdk.search.facet.FacetRequest.SortBy;
import com.attivio.sdk.search.query.Query;
import com.attivio.sdk.search.query.QueryString;
import com.attivio.sdk.server.annotation.ConfigurationOption;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.annotation.ScannerInfo;

/**
 * A simple sample scanner runs a streaming query and exports the results to a
 * CSV file.
 */
@ScannerInfo(suggestedWorkflow = "ingest")
@ConfigurationOptionInfo(displayName = "CSV Export Scanner", description = "A simple sample scanner runs a streaming query and exports the results to a CSV file",

		groups = { @ConfigurationOptionInfo.Group(path = ConfigurationOptionInfo.SCANNER, propertyNames = {
				"queryString" }) })
public class CsvExporter implements DataSourceScanner {
	private static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
	private static final String SEP = ",";
	private static final String SUBSEP = "|";

	private String queryString;
	private List<String> fieldsToInclude;
	private String outputPath;
	private String fieldSeparator = SEP;
	private String multiValueSeparator = SUBSEP;
	private String searchWorkflow = "search";

	@ConfigurationOption(optionLevel = ConfigurationOption.OptionLevel.Required)
	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	@ConfigurationOption(formEntryClass = ConfigurationOption.STRING_LIST)
	public List<String> getfieldsToInclude() {
		return fieldsToInclude;
	}

	public void setfieldsToInclude(List<String> fieldsToInclude) {
		this.fieldsToInclude = fieldsToInclude;
	}

	@ConfigurationOption(optionLevel = ConfigurationOption.OptionLevel.Required)
	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getFieldSeparator() {
		return fieldSeparator;
	}

	public void setFieldSeparator(String fieldSeparator) {
		this.fieldSeparator = fieldSeparator;
	}

	public String getMultiValueSeparator() {
		return multiValueSeparator;
	}

	public void setMultiValueSeparator(String multiValueSeparator) {
		this.multiValueSeparator = multiValueSeparator;
	}

	@ConfigurationOption(optionLevel = ConfigurationOption.OptionLevel.Required)
	public String getSearchWorkflow() {
		return searchWorkflow;
	}

	public void setSearchWorkflow(String searchWorkflow) {
		this.searchWorkflow = searchWorkflow;
	}

	protected List<String> getFields(SearchClient searcher, Query query) throws AttivioException {

		List<String> fields = new ArrayList<String>();
		Set<String> fieldsToInclude = new HashSet<String>();
		if (getfieldsToInclude() != null) {
			fieldsToInclude.addAll(getfieldsToInclude());
		}
		QueryRequest req = new QueryRequest(query);
		req.setRows(0);
		req.setRelevancyModelName("noop");
		FacetRequest facet = new FacetRequest(FieldNames.FIELD_NAMES);
		facet.setMaxBuckets(Integer.MAX_VALUE);
		facet.setSortOrder(SortBy.VALUE);
		req.setFacets(facet);
		QueryResponse res = searcher.search(req);
		if (res.getFacet(FieldNames.FIELD_NAMES) != null) {
			for (FacetBucket facetBucket : res.getFacet(FieldNames.FIELD_NAMES)) {
				if (fieldsToInclude.contains(facetBucket.getDisplayValue())) {
					fields.add(facetBucket.getDisplayValue());
				}
			}
		}
		return fields;
	}

	protected void appendFieldHeader(StringBuilder csv, List<String> fields) {
		csv.append("\"AIE_DOC_ID\"").append(getFieldSeparator());
		for (String field : fields) {
			csv.append('"').append(field.trim()).append('"').append(getFieldSeparator());
		}
	}

	protected void appendFieldValues(StringBuilder csv, SearchDocument document, List<String> fields) {
		csv.append('"').append(document.getId().trim().replace("\"", "")).append('"').append(getFieldSeparator());
		for (String field : fields) {
			String value = (document.containsField(field))
					? ApiHelper.getFieldStringValue(document.getField(field), getMultiValueSeparator())
					: "";
			csv.append('"').append(value.trim().replace("\"", "")).append('"').append(getFieldSeparator());
		}
	}

	protected void write(PrintWriter writer, String csvString) {
		writer.println(csvString);
	}

	@Override
	public void start(String name, DocumentPublisher publisher) throws AttivioException {
		PrintWriter writer = null;
		try {
			File out = new File(getOutputPath());
			if (!out.exists()) out.mkdirs();
			writer = new PrintWriter(getOutputPath() + "/" + DF.format(new Date()) + ".csv", "UTF-8"); 
			//AieClientFactory clientFactory = new DefaultAieClientFactory();
			SearchClient searcher = ApiHelper.getSearchClient();
			searcher.setClientWorkflow(searchWorkflow);
			QueryRequest request = new QueryRequest();
			Query query = new QueryString(getQueryString(), QueryLanguages.ADVANCED);
			request.setRelevancyModelName("noop");
			request.setQuery(query);
			request.setRows(Long.MAX_VALUE);
			request.setJoinRollupMode(JoinRollupMode.TREE);
			StreamingQueryRequest sRequest = new StreamingQueryRequest(request, DocumentStreamingMode.FULL_DOCUMENTS);
			StreamingQueryResponse sResponse = searcher.search(sRequest);
			StringBuilder csv = new StringBuilder();
			List<String> fields = getFields(searcher, query);
			appendFieldHeader(csv, fields);
			write(writer, csv.toString());
			for (SearchDocument document : sResponse.getDocuments()) {
				if (publisher.isStopped()) {
					throw new AttivioException(ConnectorError.CRAWL_STOPPED, "Crawl stopped");
				}
				csv = new StringBuilder();
				appendFieldValues(csv, document, fields);
				write(writer, csv.toString());
			}
		} catch (Exception exception) {
			throw new AttivioException(PlatformError.CODE_ERROR, exception, "Error in csv export scanner");
		} finally {
			if (writer != null) {
				writer.flush();
				writer.close();
			}
		}
	}

	@Override
	public void validateConfiguration() throws AttivioException {
		if (queryString == null)
			throw new AttivioException(ConnectorError.CONFIGURATION_ERROR, "Query String was not configured");
	}
}
