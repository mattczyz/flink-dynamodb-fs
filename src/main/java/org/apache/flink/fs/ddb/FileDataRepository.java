package org.apache.flink.fs.ddb;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodbv2.datamodeling.QueryResultPage;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class FileDataRepository {

    private static final Logger LOG = LoggerFactory.getLogger(FileDataRepository.class);

    private final DynamoDBMapper mapper;

    public FileDataRepository(DynamoDBMapper mapper) {
        this.mapper = mapper;
    }

    public void save(String file, long position, byte[] bytes) {
        FileData data = new FileData(file, position, ByteBuffer.wrap(bytes));
        LOG.debug("Saved file fragment: id: {}, position {}, bytes: {}", data.getId(), data.getPos(), bytes.length);
        mapper.save(data);
    }

    public FileData load(String dataId, long position) {
        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":val1", new AttributeValue().withS(dataId));
        eav.put(":val2", new AttributeValue().withN(Long.toString(position - DDBFileSystem.ITEM_SIZE)));
        eav.put(":val3", new AttributeValue().withN(Long.toString(position)));

        QueryResultPage<FileData> result =
                mapper.queryPage(
                        FileData.class,
                        new DynamoDBQueryExpression<FileData>()
                                .withKeyConditionExpression("id = :val1 and pos between :val2 and :val3")
                                .withScanIndexForward(false)
                                .withExpressionAttributeValues(eav)
                                .withLimit(1));

        LOG.debug("Loading file fragment: id: {}, position {}, result size: {} ",
                position,
                dataId,
                result.getResults().size());

        return result.getResults().get(0);
    }

    public FileData loadLast(String dataId) {
        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":val1", new AttributeValue().withS(dataId));

        QueryResultPage<FileData> result =
                mapper.queryPage(
                        FileData.class,
                        new DynamoDBQueryExpression<FileData>()
                                .withKeyConditionExpression("id = :val1")
                                .withScanIndexForward(false)
                                .withExpressionAttributeValues(eav)
                                .withLimit(1));

        LOG.debug(
                "Loading LAST file fragment: id: {}, result size: {} ", dataId, result.getResults().size());

        return result.getResults().get(0);
    }

    public void delete(String dataId) {
        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":val1", new AttributeValue().withS(dataId));

        PaginatedQueryList<FileData> result =
                mapper.query(
                        FileData.class,
                        new DynamoDBQueryExpression<FileData>()
                                .withKeyConditionExpression("id = :val1")
                                .withExpressionAttributeValues(eav));

        mapper.batchDelete(result);
        LOG.debug("Deleted data: filename: {}", dataId);
    }
}
