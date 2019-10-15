package com.wang.test;

import com.wang.utils.HDFSUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class HadoopUtilsTest {

    @Test
    public void testListFiles() {
        HDFSUtils.listFiles(true);
    }

    @Test
    public void testUpload() {
        HDFSUtils.upload("/home/rose/soft/test.txt", "/input");
    }

    @Test
    public void testDownload() {
        HDFSUtils.download("/input/test.txt", "/home/rose/");
    }

    @Test
    public void testMkdir() {
        HDFSUtils.mkdir("/test");
    }

    @Test
    public void readFile() {
        log.info(HDFSUtils.readFile("/test/test.txt"));
    }

    @Test
    public void deleteFile() {
        log.info(String.valueOf(HDFSUtils.deleteFile("/input/test.txt")));
    }

    @Test
    public void createFile() {
        HDFSUtils.createFile("/input/test.txt", "这是追加的内容");
    }
}
