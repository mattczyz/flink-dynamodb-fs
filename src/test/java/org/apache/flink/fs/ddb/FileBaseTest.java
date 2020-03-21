package org.apache.flink.fs.ddb;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import org.apache.flink.fs.ddb.rule.LocalDbRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;

public class FileBaseTest {

    @ClassRule
    public static LocalDbRule dynamoDB = new LocalDbRule();

    public static DynamoDBMapper mapperMeta;
    public static DynamoDBMapper mapperData;
    public static AmazonDynamoDB amazonDynamoDB;

    @BeforeClass
    public static void setupClass() {
        amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(
                        new AWSStaticCredentialsProvider(new BasicAWSCredentials("key", "pass")))
                .withEndpointConfiguration(
                        new AwsClientBuilder
                                .EndpointConfiguration("http://localhost:" + LocalDbRule.port + "/", "eu-west-1"))
                .build();

        mapperMeta = createMapper("default-meta");
        mapperData = createMapper("default-data");
        createTable(FileMeta.class, mapperMeta);
        createTable(FileData.class, mapperData);
    }

    private static DynamoDBMapper createMapper(String tableName) {
        return new DynamoDBMapper(
                amazonDynamoDB,
                new DynamoDBMapperConfig.Builder()
                        .withTableNameOverride(
                                DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(tableName))
                        .build());
    }

    private static <T> void createTable(Class<T> clazz, DynamoDBMapper mapper) {
        CreateTableRequest tableRequest = mapper.generateCreateTableRequest(clazz);
        tableRequest.setProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
        amazonDynamoDB.createTable(tableRequest);
    }
}
