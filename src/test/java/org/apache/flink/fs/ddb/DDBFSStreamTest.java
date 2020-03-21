package org.apache.flink.fs.ddb;

import org.apache.flink.shaded.guava18.com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class DDBFSStreamTest extends FileBaseTest {
    @Test
    public void dataInputShouldBeEqualOutput() throws IOException {
        DDBFSDataOutputStream output = new DDBFSDataOutputStream("file1", new FileDataRepository(mapperData));
        byte[] array = new byte[800 * 1024];
        new Random().nextBytes(array);

        output.write(array);
        output.close();

        DDBFSDataInputStream input = new DDBFSDataInputStream("file1", new FileDataRepository(mapperData));
        byte[] result = ByteStreams.toByteArray(input);
        Assert.assertArrayEquals(array, result);
    }
}
