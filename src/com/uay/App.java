package com.uay;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.util.Date;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class App {

    public static final String INDEX = "twitter";
    public static final String TYPE = "tweet";

    public static void main(String[] args) throws IOException {
        Client client = createClient();

        for (int i = 2; i < 50; i++) {
            indexDocument(client, "kevin" + i, "msg" + i, INDEX, TYPE, String.valueOf(i));
        }
        GetResponse getResponse = getDocument(client, "3");
        SearchResponse searchResponse = searchDocument(client);
    }

    private static SearchResponse searchDocument(Client client) {
//        SearchResponse response2 = client.prepareSearch().execute().actionGet();
        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(TYPE)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("user", "kevin3"))             // Query
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();
        System.out.println("response.getHits().getHits()[0] = " + response.getHits().getHits()[0].getSource());
        return response;
    }

    private static GetResponse getDocument(Client client, String id) {
        GetResponse getResponse = client.prepareGet(INDEX, TYPE, id)
                .setOperationThreaded(false)
                .execute()
                .actionGet();
        System.out.println(getResponse.getSource());
        return getResponse;
    }

    private static IndexResponse indexDocument(Client client, String user, String message, String index,
                                      String type, String id) throws IOException {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("user", user)
                .field("postDate", new Date())
                .field("message", message)
                .endObject();
        System.out.println(builder.string());

        IndexResponse response = client.prepareIndex(index, type, id)
                .setSource(builder)
                .execute()
                .actionGet();

        // Index name
        String _index = response.getIndex();
        System.out.println("_index = " + _index);
        // Type name
        String _type = response.getType();
        System.out.println("_type = " + _type);
        // Document ID (generated or not)
        String _id = response.getId();
        System.out.println("_id = " + _id);
        // Version (if it's the first time you index this document, you will get: 1)
        long _version = response.getVersion();
        System.out.println("_version = " + _version);
        // isCreated() is true if the document is a new one, false if it has been updated
        boolean created = response.isCreated();
        System.out.println("created = " + created);
        return response;
    }

    public static Client createClient() {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "myClusterName").build();
        TransportClient transportClient = new TransportClient(settings);
        transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        return (Client) transportClient;
    }
}
