package org.apache.flink.fs.ddb;


import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.guava18.com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class DDBFileSystemTest extends FileBaseTest {
    private FileSystem fs = new DDBFileSystem(
            new FileMetaRepository(mapperMeta),
            new FileDataRepository(mapperData)
    );

    @Test
    public void createAndOpenFile() throws IOException {
        String filename = "ddb:///namespace/file1";
        byte[] randomBytes = writeRandomData(filename);

        FSDataInputStream input = fs.open(new Path(filename));
        byte[] result = ByteStreams.toByteArray(input);
        Assert.assertArrayEquals(randomBytes, result);
    }

    @Test
    public void fileStatus() throws IOException {
        String filename = "ddb:///namespace/file2";
        byte[] randomBytes = writeRandomData(filename);

        FileStatus status = fs.getFileStatus(new Path(filename));

        Assert.assertEquals(status.getLen(), randomBytes.length);
    }

    @Test
    public void delete() throws IOException {
        String filename = "ddb:///namespace/file3";
        writeRandomData(filename);
        Assert.assertTrue(fs.exists(new Path("ddb:///namespace/file3")));
        fs.delete(new Path("ddb:///namespace/file3"), false);
        Assert.assertFalse(fs.exists(new Path("ddb:///namespace/file3")));
    }

    private byte[] writeRandomData(String filename) throws IOException {
        FSDataOutputStream output = fs.create(new Path(filename), FileSystem.WriteMode.NO_OVERWRITE);
        byte[] array = new byte[800 * 1024];
        new Random().nextBytes(array);

        output.write(array);
        output.close();
        return array;
    }
}
