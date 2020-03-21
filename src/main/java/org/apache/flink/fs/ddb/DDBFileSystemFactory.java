package org.apache.flink.fs.ddb;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;

import java.io.IOException;
import java.net.URI;

public class DDBFileSystemFactory implements FileSystemFactory {
    private Configuration flinkConfig;

    @Override
    public void configure(Configuration config) {
        flinkConfig = config;
    }

    @Override
    public String getScheme() {
        return "ddb";
    }

    @Override
    public FileSystem create(URI fsUri) throws IOException {
        return new DDBFileSystem(uriToTableName(fsUri), flinkConfig);
    }

    String uriToTableName(URI fsUri) {
        return fsUri.getPath().substring(1).replaceAll("[^a-zA-Z0-9_]", "-");
    }
}
