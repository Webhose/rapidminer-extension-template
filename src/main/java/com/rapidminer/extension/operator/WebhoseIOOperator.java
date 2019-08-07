package com.rapidminer.extension.operator;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.*;
import com.rapidminer.extension.WebhoseIOClient;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.parameter.*;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Describe what your operator does.
 *
 * @author Insert your name here
 *
 */
public class WebhoseIOOperator extends Operator {
    public static final String TOKEN_PARAMETER_TEXT = "token";
    public static final String QUERY_PARAMETER_TEXT = "query";
    public static final String MAX_REQUESTS_PARAMETER_TEXT = "max requests";
    private OutputPort exampleSetOutput = getOutputPorts().createPort("webhose set");

    /**
     * @param description
     */
    public WebhoseIOOperator(OperatorDescription description) {
        super(description);
    }

    @Override
    public void doWork() throws OperatorException {
        WebhoseIOClient webhoseClient = WebhoseIOClient.getInstance(getParameterAsString(TOKEN_PARAMETER_TEXT).trim());
        Map<String, String> queries = new HashMap<String, String>();
        queries.put("q", getParameterAsString(QUERY_PARAMETER_TEXT).trim());

        String thirty_days_ago = Long.toString(DateTime.now().minusDays(30).getMillis());
        queries.put("ts", thirty_days_ago);

        JsonElement result = null;
        try {
            result = webhoseClient.query("filterWebContent", queries);
        } catch (URISyntaxException | IOException e) {
            LogService.getRoot().log(Level.INFO, e.getMessage());
        }

        int queries_num = 1;

        List<Attribute> listOfAtts = new LinkedList<>();
        ExampleSet exampleSet;

        Attribute urlAtt = AttributeFactory.createAttribute("URL", Ontology.ATTRIBUTE_VALUE_TYPE.POLYNOMINAL);
        listOfAtts.add(urlAtt);

        Attribute titleAtt = AttributeFactory.createAttribute("Title", Ontology.ATTRIBUTE_VALUE_TYPE.POLYNOMINAL);
        listOfAtts.add(titleAtt);

        Attribute textAtt = AttributeFactory.createAttribute("Text", Ontology.ATTRIBUTE_VALUE_TYPE.POLYNOMINAL);
        listOfAtts.add(textAtt);

        Attribute authorAtt = AttributeFactory.createAttribute("Author", Ontology.ATTRIBUTE_VALUE_TYPE.POLYNOMINAL);
        listOfAtts.add(authorAtt);

        Attribute langAtt = AttributeFactory.createAttribute("Language", Ontology.ATTRIBUTE_VALUE_TYPE.POLYNOMINAL);
        listOfAtts.add(langAtt);

        Attribute publishedAtt = AttributeFactory.createAttribute("Published", Ontology.ATTRIBUTE_VALUE_TYPE.DATE_TIME);
        listOfAtts.add(publishedAtt);

        Attribute crawledAtt = AttributeFactory.createAttribute("Crawled", Ontology.ATTRIBUTE_VALUE_TYPE.DATE_TIME);
        listOfAtts.add(crawledAtt);

        Attribute uuidAtt = AttributeFactory.createAttribute("uuid", Ontology.ATTRIBUTE_VALUE_TYPE.POLYNOMINAL);
        listOfAtts.add(uuidAtt);


        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        MemoryExampleTable table = new MemoryExampleTable(listOfAtts);

        while (true) {
            if (result != null) {
                JsonArray postArray = result.getAsJsonObject().getAsJsonArray("posts");
                for (JsonElement o : postArray) {
                    double[] attributes = new double[8];
                    attributes[0] = urlAtt.getMapping().mapString(o.getAsJsonObject().get("url").getAsString());
                    attributes[1] = titleAtt.getMapping().mapString(o.getAsJsonObject().get("title").getAsString());
                    attributes[2] = textAtt.getMapping().mapString(o.getAsJsonObject().get("text").getAsString());
                    attributes[3] = authorAtt.getMapping().mapString(o.getAsJsonObject().get("author").getAsString());
                    attributes[4] = langAtt.getMapping().mapString(o.getAsJsonObject().get("language").getAsString());
                    DateTime dt = formatter.parseDateTime(o.getAsJsonObject().get("published").getAsString());
                    attributes[5] = dt.getMillis();
                    dt = formatter.parseDateTime(o.getAsJsonObject().get("crawled").getAsString());
                    attributes[6] = dt.getMillis();

                    attributes[7] = uuidAtt.getMapping().mapString(o.getAsJsonObject().get("uuid").getAsString());
                    table.addDataRow(new DoubleArrayDataRow(attributes));

                    exampleSet = table.createExampleSet();

                    exampleSetOutput.deliver(exampleSet);
                }
            }

                if (result.getAsJsonObject().get("moreResultsAvailable").getAsInt() > 0 &&
                        queries_num < getParameterAsInt(MAX_REQUESTS_PARAMETER_TEXT)) {
                    try {
                        result = webhoseClient.getNext();
                        queries_num++;
                    } catch (URISyntaxException | IOException e) {
                        LogService.getRoot().log(Level.INFO, e.getMessage());
                        break;
                    }
                } else {
                    break;
                }
        }
    }

    @Override
    public List<ParameterType> getParameterTypes(){
        List<ParameterType> types = super.getParameterTypes();

        types.add(new ParameterTypeString(
                TOKEN_PARAMETER_TEXT,
                "Your webhose.io token. Get 1 here: https://webhose.io/auth/signup ",
                false,
                false
        ));

        types.add(new ParameterTypeString(
                QUERY_PARAMETER_TEXT,
                "The boolean query for your search. Example: 'stock market' language:english",
                false,
                false
        ));

        types.add(new ParameterTypeInt(
                MAX_REQUESTS_PARAMETER_TEXT,
                "The maximum number of API calls you want to make. Each API returns up to 100 results",
                1,
                1000,
                10
        ));

        return types;
    }

}