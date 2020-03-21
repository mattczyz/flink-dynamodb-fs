package org.apache.flink.fs.ddb;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

public class FileMetaTest extends FileBaseTest {

    @Test
    public void createFile() throws IOException {
        FileMetaRepository repository = new FileMetaRepository(mapperMeta);
        repository.create("ddb:///test_file");
        FileMeta file = new FileMeta("ddb:///test_file");

        FileMeta result = mapperMeta.load(FileMeta.class, "ddb:///test_file");

        Assert.assertEquals(file.getFilename(), result.getFilename());
        Assert.assertNotNull(result.getDataId());

        FileMeta resultNull = mapperMeta.load(FileMeta.class, "ddb:///test_file2");
        Assert.assertNull(resultNull);
    }

    @Test
    public void findFiles() throws IOException {
        FileMetaRepository repository = new FileMetaRepository(mapperMeta);
        repository.create("ddb:///other_test_file1");
        repository.create("ddb:///this_test_file1");
        repository.create("ddb:///this_test_file2");

        ArrayList<FileMeta> result = new ArrayList<FileMeta>(repository.find("ddb:///this_test_file"));
        Assert.assertEquals(2, result.size());
    }

    @Test
    public void deleteFile() throws IOException {
        FileMetaRepository repository = new FileMetaRepository(mapperMeta);
        repository.create("ddb:///delete_test_file");
        Assert.assertNotNull(repository.load("ddb:///delete_test_file"));
        repository.delete("ddb:///delete_test_file");
        Assert.assertNull(repository.load("ddb:///delete_test_file"));
    }
}
