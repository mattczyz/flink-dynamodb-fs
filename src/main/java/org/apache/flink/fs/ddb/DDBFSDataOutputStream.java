package org.apache.flink.fs.ddb;

import org.apache.flink.core.fs.FSDataOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class DDBFSDataOutputStream extends FSDataOutputStream {

    final private String dataId;
    final private FileDataRepository fileDataRepository;
    private ByteArrayOutputStream output = new ByteArrayOutputStream();
    private long posCurr = 0;
    private long posLast = 0;

    public DDBFSDataOutputStream(String dataId, FileDataRepository fileDataRepository) {
        this.dataId = dataId;
        this.fileDataRepository = fileDataRepository;
    }

    @Override
    public long getPos() throws IOException {
        return posCurr;
    }

    @Override
    public void write(int b) throws IOException {
        output.write(b);
        posCurr += 1;
        if (output.size() == DDBFileSystem.ITEM_SIZE) {
            flush();
        }
    }

    @Override
    public void flush() throws IOException {
        if (output.size() > 0) {
            fileDataRepository.save(dataId, posLast, output.toByteArray());
            posLast = posCurr;
            output = new ByteArrayOutputStream();
        }
    }

    @Override
    public void sync() throws IOException {
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
