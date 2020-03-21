package org.apache.flink.fs.ddb;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class DDBFileSystemFactoryTest {

    @Test
    public void uriToTableName() throws IOException, URISyntaxException {
        DDBFileSystemFactory factory = new DDBFileSystemFactory();
        String tableName = factory.uriToTableName(new URI("ddb:///namespace/location"));

        Assert.assertEquals("namespace-location", tableName);
    }
}
