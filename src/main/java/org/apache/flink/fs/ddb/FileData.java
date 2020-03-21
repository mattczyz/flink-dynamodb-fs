package org.apache.flink.fs.ddb;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import java.nio.ByteBuffer;

@DynamoDBTable(tableName = "")
public class FileData {

    private String id;
    private Long pos;
    private ByteBuffer bytes;

    public FileData() {
    }

    public FileData(String id, Long pos, ByteBuffer bytes) {
        this.id = id;
        this.pos = pos;
        this.bytes = bytes;
    }

    @DynamoDBHashKey
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @DynamoDBRangeKey
    public Long getPos() {
        return pos;
    }

    public void setPos(Long pos) {
        this.pos = pos;
    }

    @DynamoDBAttribute
    public ByteBuffer getBytes() {
        return bytes;
    }

    public void setBytes(ByteBuffer bytes) {
        this.bytes = bytes;
    }
}
