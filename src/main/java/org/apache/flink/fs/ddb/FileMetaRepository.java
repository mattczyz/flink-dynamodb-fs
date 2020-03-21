package org.apache.flink.fs.ddb;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class FileMetaRepository {

    private static final Logger LOG = LoggerFactory.getLogger(FileMetaRepository.class);

    final private DynamoDBMapper mapper;

    public FileMetaRepository(DynamoDBMapper mapper) {
        this.mapper = mapper;
    }

    public FileMeta create(String filename) {
        FileMeta meta = new FileMeta(filename);

        mapper.save(meta);

        LOG.debug("Created file: filename: {}", filename);

        return load(filename);
    }

    public FileMeta create(String filename, String dataId) {
        FileMeta meta = new FileMeta(filename, dataId);

        mapper.save(meta);

        LOG.debug("Created file: filename: {}, dataId {}", filename, dataId);

        return load(filename);
    }

    public void delete(String filename) {
        FileMeta meta = new FileMeta(filename);
        mapper.delete(meta);
        LOG.debug("Deleted file: filename: {}", filename);
    }

    public PaginatedScanList<FileMeta> find(String path) {
        Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
        eav.put(":val1", new AttributeValue().withS(path));

        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
                .withFilterExpression("begins_with(filename,:val1)").withExpressionAttributeValues(eav);

        return mapper.scan(FileMeta.class, scanExpression);
    }

    public FileMeta load(String filename) {
        return mapper.load(FileMeta.class, filename);
    }

    public Boolean exists(String filename) {
        return mapper.load(FileMeta.class, filename) != null;
    }
}
