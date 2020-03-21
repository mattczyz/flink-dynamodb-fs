package org.apache.flink.fs.ddb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;

public class DDBFileSystem extends FileSystem {
    public static final int ITEM_SIZE = 399 * 1024;
    private static final Logger LOG = LoggerFactory.getLogger(DDBFileSystem.class);
    private static final String FLINK_DDB_TABLE_READ_CAPACITY = "fs.ddb.table.read.capacity";
    private static final String FLINK_DDB_TABLE_WRITE_CAPACITY = "fs.ddb.table.write.capacity";
    private final URI workingDir;
    private final URI homeDir;
    private final FileMetaRepository repositoryMeta;
    private final FileDataRepository repositoryData;

    public DDBFileSystem(FileMetaRepository repositoryMeta, FileDataRepository repositoryData) {
        this.repositoryMeta = repositoryMeta;
        this.repositoryData = repositoryData;
        this.workingDir = new File(System.getProperty("user.dir")).toURI();
        this.homeDir = new File(System.getProperty("user.home")).toURI();
    }

    public DDBFileSystem(String tablePrefix, Configuration flinkConfig) {
        AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient();

        DynamoDBMapper mapperMeta = buildDynamoDBMapper(amazonDynamoDB, tablePrefix + "-meta");
        DynamoDBMapper mapperData = buildDynamoDBMapper(amazonDynamoDB, tablePrefix + "-data");

        createTable(amazonDynamoDB, FileMeta.class, mapperMeta, flinkConfig);
        createTable(amazonDynamoDB, FileData.class, mapperData, flinkConfig);

        this.repositoryMeta = new FileMetaRepository(mapperMeta);
        this.repositoryData = new FileDataRepository(mapperData);

        this.workingDir = new File(System.getProperty("user.dir")).toURI();
        this.homeDir = new File(System.getProperty("user.home")).toURI();
    }

    private <T> void createTable(AmazonDynamoDB amazonDynamoDB, Class<T> clazz, DynamoDBMapper mapper, Configuration flinkConfig) {
        CreateTableRequest tableRequest = mapper.generateCreateTableRequest(clazz);
        List<String> tables = amazonDynamoDB.listTables().getTableNames();

        if (!tables.contains(tableRequest.getTableName())) {
            LOG.info("Creating table: {}", tableRequest.getTableName());
            Long readCapacity = flinkConfig.getLong(FLINK_DDB_TABLE_READ_CAPACITY, 1L);
            Long writeCapacity = flinkConfig.getLong(FLINK_DDB_TABLE_WRITE_CAPACITY, 1L);
            tableRequest.setProvisionedThroughput(new ProvisionedThroughput(readCapacity, writeCapacity));
            amazonDynamoDB.createTable(tableRequest);
        }
    }

    private DynamoDBMapper buildDynamoDBMapper(AmazonDynamoDB amazonDynamoDB, String metaTable) {
        return new DynamoDBMapper(
                amazonDynamoDB,
                new DynamoDBMapperConfig.Builder()
                        .withTableNameOverride(
                                DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(metaTable))
                        .build());
    }

    @Override
    public Path getWorkingDirectory() {
        return new Path(workingDir);
    }

    @Override
    public Path getHomeDirectory() {
        return new Path(homeDir);
    }

    @Override
    public URI getUri() {
        return URI.create("ddb:///");
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        DDBFileStatus status = null;
        if (repositoryMeta.exists(f.getPath())) {
            status = new DDBFileStatus(f.getPath(), repositoryMeta, repositoryData);
        }

        return status;
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
        return new BlockLocation[0];
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        FileMeta file = repositoryMeta.load(f.getPath());

        if (file == null) {
            throw new IOException();
        }

        return new DDBFSDataInputStream(file.getDataId(), repositoryData);
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        return this.open(f, 0);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        return new FileStatus[0];
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        if (recursive) {
            PaginatedScanList<FileMeta> results = repositoryMeta.find(f.getPath());
            results.forEach(m -> {
                repositoryMeta.delete(m.getFilename());
                repositoryData.delete(m.getDataId());
            });
        } else {
            FileMeta m = repositoryMeta.load(f.getPath());
            if (m != null) {
                repositoryMeta.delete(m.getFilename());
                repositoryData.delete(m.getDataId());
            }
        }

        LOG.info("Deleted: " + f.getPath() + ", recursive: " + recursive);
        return true;
    }

    @Override
    public boolean mkdirs(Path f) throws IOException {
        return true;
    }

    @Override
    public FSDataOutputStream create(Path f, WriteMode overwriteMode) throws IOException {
        if (repositoryMeta.load(f.getPath()) != null) {
            throw new IOException();
        }

        FileMeta file = repositoryMeta.create(f.getPath());

        LOG.info("Created file: filename: {}", f.getPath());
        return new DDBFSDataOutputStream(file.getDataId(), repositoryData);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        FileMeta file = repositoryMeta.load(src.getPath());

        if (file == null) {
            throw new IOException();
        }

        repositoryMeta.create(dst.getPath(), file.getDataId());

        return true;
    }

    @Override
    public boolean isDistributedFS() {
        return false;
    }

    @Override
    public FileSystemKind getKind() {
        return FileSystemKind.OBJECT_STORE;
    }
}
