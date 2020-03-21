package org.apache.flink.fs.ddb;

import org.apache.flink.core.fs.FSDataInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DDBFSDataInputStream extends FSDataInputStream {
    final private String dataId;
    final private FileDataRepository fileDataRepository;

    private ByteBuffer buffer;
    private long bufferMin;
    private long bufferMax;
    private long bufferPos = 0;
    private long posMax;
    private long posCurr = 0;

    public DDBFSDataInputStream(String dataId, FileDataRepository fileDataRepository) {
        this.dataId = dataId;
        this.fileDataRepository = fileDataRepository;
        FileData fileData = fileDataRepository.loadLast(dataId);
        posMax = fileData.getPos() + fileData.getBytes().array().length;
    }

    @Override
    public void seek(long l) throws IOException {
        if (l == 0) {
            loadBuffer(l);
        } else if (l < bufferMin || l >= bufferMax) {
            loadBuffer(l);
        }
    }

    private void loadBuffer(long l) throws IOException {
        FileData fileData = fileDataRepository.load(dataId, l);
        if (fileData == null) {
            throw new IOException();
        } else {
            buffer = fileData.getBytes();
            bufferMin = fileData.getPos();
            bufferMax = fileData.getPos() + fileData.getBytes().array().length;
            bufferPos = l - bufferMin;
        }
    }

    @Override
    public long getPos() {
        return posCurr;
    }

    @Override
    public int read() throws IOException {
        if (posCurr == posMax) {
            return -1;
        } else {
            seek(posCurr);
            byte result = buffer.get((int) bufferPos);
            posCurr += 1;
            bufferPos += 1;
            return Byte.toUnsignedInt(result);
        }
    }
}
