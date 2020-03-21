package org.apache.flink.fs.ddb;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

public class DDBFileStatus implements FileStatus {

    private final String filename;
    private final FileDataRepository repositoryData;
    private final FileMeta fileMeta;

    public DDBFileStatus(String filename, FileMetaRepository repositoryMeta, FileDataRepository repositoryData) {
        this.filename = filename;
        this.repositoryData = repositoryData;
        this.fileMeta = repositoryMeta.load(filename);
    }

    @Override
    public long getLen() {
        FileData tail = repositoryData.loadLast(fileMeta.getDataId());
        return tail.getPos() + tail.getBytes().array().length;
    }

    @Override
    public long getBlockSize() {
        return getLen();
    }

    @Override
    public short getReplication() {
        return 1;
    }

    @Override
    public long getModificationTime() {
        return 0;
    }

    @Override
    public long getAccessTime() {
        return 0;
    }

    @Override
    public boolean isDir() {
        return false;
    }

    @Override
    public Path getPath() {
        return new Path(filename);
    }
}
