/**
 * 
 */
package com.dynamo.madhu;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.util.Tables;

/**
 * @author madhukar
 *
 */
public class CRUDOperations {

    private static AmazonDynamoDBClient dynamoDB;

    private static void init() throws Exception {

        File file = new File("src/resources/AwsCredential.properties");
        AWSCredentials credentials = new PropertiesCredentials(file);

        dynamoDB = new AmazonDynamoDBClient(credentials);
        dynamoDB.setRegion(Region.getRegion(Regions.AP_SOUTHEAST_1));
    }

    static String productCatalogName = "ProductCatalog";

    public static void main(String[] args) throws Exception {

        init();
        insertData(productCatalogName);
        getData(productCatalogName);
        updateData(productCatalogName);
    }

    private static void insertData(String tableName) {

        ListTablesResult listTablesResult = dynamoDB.listTables();
        System.out.println(listTablesResult.getTableNames());

        if (!Tables.doesTableExist(dynamoDB, productCatalogName)) {
            CreateTableRequest request = new CreateTableRequest().withTableName(productCatalogName);
            request.withKeySchema(new KeySchemaElement().withAttributeName("customerId").withKeyType(KeyType.HASH));

            request.withAttributeDefinitions(new AttributeDefinition().withAttributeName("customerId")
                    .withAttributeType(ScalarAttributeType.S));

            request.setProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(5L)
                    .withWriteCapacityUnits(2L));
            dynamoDB.createTable(request);
        } else {
            dynamoDB.putItem(new PutItemRequest().withTableName(productCatalogName)
                    .addItemEntry("customerId", new AttributeValue("100"))
                    .addItemEntry("Name", new AttributeValue("shoe"))
                    .addItemEntry("Color", new AttributeValue(Arrays.asList("red", "green")))
                    .addItemEntry("Price", new AttributeValue("1200")));
            System.out.println("Item Inserted");
        }

    }

    private static void getData(String tableName) {

        GetItemRequest getItemRequest = new GetItemRequest();
        getItemRequest.addKeyEntry("customerId", new AttributeValue("100"));
        getItemRequest.setTableName(tableName);
        GetItemResult getItemResult = dynamoDB.getItem(getItemRequest);
        Map<String, AttributeValue> map = getItemResult.getItem();
        System.out.println(map.get("Name"));
        System.out.println(map.get("Color"));

    }

    private static void updateData(String tableName) {

        UpdateItemRequest updateItemRequest = new UpdateItemRequest();
        updateItemRequest.setTableName(tableName);
        updateItemRequest.addKeyEntry("customerId", new AttributeValue("100"));
        updateItemRequest.addAttributeUpdatesEntry("Color",
                new AttributeValueUpdate(new AttributeValue(Arrays.asList("red", "green", "white","orange")),
                        AttributeAction.PUT));
        UpdateItemResult itemResult = dynamoDB.updateItem(updateItemRequest);
        itemResult.getAttributes();
        System.out.println("Updates successfully..");
        getData(tableName);
    }

}
