package com.wang.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

@Slf4j
public class HDFSUtils {
    private static Configuration configuration;
    private static FileSystem fs;

    //私有构造方法，禁止工具类被实例化
    private HDFSUtils() {
    }

    /**
     * HDFS相关设置
     */
    static {
        configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://txrose:9000");
        configuration.set("dfs.support.append", "true");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        configuration.set("dfs.client.use.datanode.hostname", "true");
        try {
            fs = FileSystem.newInstance(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 遍历hdfs目录
     *
     * @param recursive 是否递归遍历
     */
    public static void listFiles(Boolean recursive) {
        try {
            // true 表示递归查找 false 不进行递归查找
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), recursive);
            while (iterator.hasNext()) {
                LocatedFileStatus next = iterator.next();
                log.info(String.valueOf(next.getPath()));
            }
            System.out.println("----------------------------------------------------------");
            FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
            for (int i = 0; i < fileStatuses.length; i++) {
                FileStatus fileStatus = fileStatuses[i];
                log.info(String.valueOf(fileStatus.getPath()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 上传文件到hdfs
     *
     * @param localPath  本地路径
     * @param originPath hdfs路径
     */
    public static void upload(String localPath, String originPath) {
        try {
            long currentTimeMillis = System.currentTimeMillis();
            fs.copyFromLocalFile(new Path(localPath), new Path(originPath));
            long time = System.currentTimeMillis() - currentTimeMillis;
            log.info("upload success,use time:" + time / 1000 + " s");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 从 hdfs 下载文件
     *
     * @param originPath
     * @param localPath
     */
    public static void download(String originPath, String localPath) {
        try {
            log.info("start downloading...");
            fs.copyToLocalFile(new Path(originPath), new Path(localPath));
            log.info("success");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 在 hdfs根目录创建文件夹
     *
     * @param path
     */
    public static void mkdir(String path) {
        try {
            fs.mkdirs(new Path(path));
            log.info("success");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建文件,如果已经存在则追加内容
     *
     * @param fileName
     * @param fileContent
     */
    public static void createFile(String fileName, String fileContent) {
        //判断文件是否存在
        Path path = new Path(fileName);
        try {
            byte[] bytes = fileContent.getBytes("UTF-8");
            if (fs.exists(path)) {
                //已经存在，追加到文件
                try (
                        FSDataOutputStream outputStream = fs.append(path);
                ) {
                    outputStream.write(bytes);
                }
                return;
            }
            //不存在创建
            try (FSDataOutputStream outputStream = fs.create(new Path(fileName))) {
                outputStream.write(bytes);
                log.info("new file \t" + configuration.get("fs.default.name")
                        + fileName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    /**
     * 读取文件内容
     *
     * @param fileName
     * @return
     */
    public static String readFile(String fileName) {
        try (
                FSDataInputStream inputStream = fs.open(new Path(fileName));
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))
        ) {
            StringBuilder sb = new StringBuilder();
            String str = "";
            while (null != (str = bufferedReader.readLine())) {
                sb.append(str);
                sb.append("\n");
            }

            return sb.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("文件读取失败");
    }

    /**
     * 删除文件
     *
     * @param fileName
     * @return boolean
     */
    public static boolean deleteFile(String fileName) {
        try {
            return fs.deleteOnExit(new Path(fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}
