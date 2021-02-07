package com.itheima.canal_demo;


import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * canal客户端程序
 * cs 架构的程序
 * client/server
 */
public class CanalClientDemo {
    public static void main(String[] args) {
        //1：创建连接。对应的是单机环境的操作的
        //CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop01", 11111), "example", "", "");
        //连接canalserver端集群环境，对应的是canal的集群环境的配置操作的.用户名称和密码是存在问题的，需要进行关注。
        CanalConnector connector = CanalConnectors.newClusterConnector("hadoop01:2181,hadoop02:2181,hadoop03:2181", "example", "canal", "canal");
        //指定一次性获取数据的条数
        int batchSize = 1000;
        boolean running = true;
        try {
            //while (running) {
            //2：建立连接
            connector.connect();
            //回滚上次的get请求，重新获取数据
            connector.rollback();
            //订阅匹配日志
            connector.subscribe("itcast_shop.*");
            while (running) {
                //3：获取数据
                //批量拉取binlog日志。一次拉取多条数据
                //需要手动的保存数据的，这样的话可以保证数据的存储操作的。
                Message message = connector.getWithoutAck(batchSize);
                //获取batchid
                long batchId = message.getId();
                //获取binlog数据的条数
                int size = message.getEntries().size();
                if (size == 0 || size == -1) {
                    //表示没有获取到数据
                } else {
                    //有数据。打印数据
                    printSummary(message);
                    //将binlog日志转换成json字符串输出
                    //String json = binlogToJson(message);  //这种方式不是我们的最终方案，我们需要将protobuf二进制数据写入到kafka中
                    //System.out.println(json);
                    byte[] bytes = binlogToProtoBuf(message);
                    // 将转换之后的字节码的数据发送到kafka的集群中进行操作管理。
                    /*for (byte b:bytes){
                        System.out.print(b);
                    }*/
                    /**
                     * kafka中可以书写的字符串的类型
                     * 1.字符串的类型
                     * 2.二进制的字节码信息
                     *  从上面的mysql的binlog解析出来的数据，杂项太多了，需要进行精简处理操作
                     *  kakfa中不需要这么多的垃圾数据的。数据量太大的话，对应的io资源花费的也是比较的多的
                     *  可以使用protobuf的方式来精简存储资源和减少网络资源的消耗的。
                     *  需要将存储的数据格式化成为资源占用比较少的数据的。
                     *  kakfa可以使用protobuf的格式化的操作来减少数据的存储占用的。这个很关键的。
                     *  protobuf的作用：格式化数据的序列化和反序列化的，常用于rpc的操作的。
                     *  适合数据存储和rpc通讯操作的。和开发语言无关的。
                     *  完全可以抛弃对应的fastjson的序列化的操作方式，从而使用protobuf的序列化方式实现序列化的操作的
                     *  以后所有的操作，推荐使用protobuf的方式实现操作的。
                     *  可以节省网络资源和存储资源进行操作管理实现。
                     *  使用protobuf进行操作的话，需要书写相关的protobuf的语法操作的。
                     *  canel获取数据的话,可以采用netty连接拉取自己对应的数据信息的。可以使用netty连接的方式实现相关的操作的。
                     *
                     * */
                }
                connector.ack(batchId); // 提交确认
            }
            //}
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            //4：断开连接
            connector.disconnect();
        }
    }

    //说明有数据
    private static void printSummary(Message message) {
        // 遍历整个batch中的每个binlog实体，如果拉取到多少条数据就循环多少次
        for (CanalEntry.Entry entry : message.getEntries()) {
            // 事务开始
            if(entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            // 获取binlog文件名
            String logfileName = entry.getHeader().getLogfileName();
            // 获取logfile的偏移量
            long logfileOffset = entry.getHeader().getLogfileOffset();
            // 获取sql语句执行时间戳
            long executeTime = entry.getHeader().getExecuteTime();
            // 获取数据库名
            String schemaName = entry.getHeader().getSchemaName();
            // 获取表名
            String tableName = entry.getHeader().getTableName();
            // 获取事件类型 insert/update/delete
            String eventTypeName = entry.getHeader().getEventType().toString().toLowerCase();
            System.out.println("logfileName" + ":" + logfileName);
            System.out.println("logfileOffset" + ":" + logfileOffset);
            System.out.println("executeTime" + ":" + executeTime);
            System.out.println("schemaName" + ":" + schemaName);
            System.out.println("tableName" + ":" + tableName);
            System.out.println("eventTypeName" + ":" + eventTypeName);
            CanalEntry.RowChange rowChange = null;
            try {
                // 获取存储数据，并将二进制字节数据解析为RowChange实体
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
            // 迭代每一条变更数据
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                // 判断是否为删除事件
                if(entry.getHeader().getEventType() == CanalEntry.EventType.DELETE) {
                    System.out.println("---delete---");
                    printColumnList(rowData.getBeforeColumnsList());
                    System.out.println("---");
                }
                // 判断是否为更新事件
                else if(entry.getHeader().getEventType() == CanalEntry.EventType.UPDATE) {
                    System.out.println("---update---");
                    printColumnList(rowData.getBeforeColumnsList());
                    System.out.println("---");
                    printColumnList(rowData.getAfterColumnsList());
                }
                // 判断是否为插入事件
                else if(entry.getHeader().getEventType() == CanalEntry.EventType.INSERT) {
                    System.out.println("---insert---");
                    printColumnList(rowData.getAfterColumnsList());
                    System.out.println("---");
                }
            }
        }
    }

    // 打印所有列名和列值
    private static void printColumnList(List<CanalEntry.Column> columnList) {
        for (CanalEntry.Column column : columnList) {
            System.out.println(column.getName() + "\t" + column.getValue());
        }
    }

    // binlog解析为json字符串
    private static String binlogToJson(Message message) throws InvalidProtocolBufferException {
        // 1. 创建Map结构保存最终解析的数据
        Map rowDataMap = new HashMap<String, Object>();
        // 2. 遍历message中的所有binlog实体
        for (CanalEntry.Entry entry : message.getEntries()) {
            // 只处理事务型binlog
            if(entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
                    entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            // 获取binlog文件名
            String logfileName = entry.getHeader().getLogfileName();
            // 获取logfile的偏移量
            long logfileOffset = entry.getHeader().getLogfileOffset();
            // 获取sql语句执行时间戳
            long executeTime = entry.getHeader().getExecuteTime();
            // 获取数据库名
            String schemaName = entry.getHeader().getSchemaName();
            // 获取表名
            String tableName = entry.getHeader().getTableName();
            // 获取事件类型 insert/update/delete
            String eventType = entry.getHeader().getEventType().toString().toLowerCase();
            rowDataMap.put("logfileName", logfileName);
            rowDataMap.put("logfileOffset", logfileOffset);
            rowDataMap.put("executeTime", executeTime);
            rowDataMap.put("schemaName", schemaName);
            rowDataMap.put("tableName", tableName);
            rowDataMap.put("eventType", eventType);
            // 封装列数据
            Map columnDataMap = new HashMap<String, Object>();
            // 获取所有行上的变更，对应的是数据的变更信息的，体现的是数据从那个方面的数据修改操作
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            List<CanalEntry.RowData> columnDataList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : columnDataList) {
                if(eventType.equals("insert") || eventType.equals("update")) {
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue());
                    }
                }
                else if(eventType.equals("delete")) {
                    for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue());
                    }
                }
            }
            rowDataMap.put("columns", columnDataMap);
        }
        return JSON.toJSONString(rowDataMap);
    }

    // binlog解析为ProtoBuf
    private static byte[] binlogToProtoBuf(Message message) throws InvalidProtocolBufferException {
        // 1. 构建CanalModel.RowData实体
        CanalModel.RowData.Builder rowDataBuilder = CanalModel.RowData.newBuilder();
        // 1. 遍历message中的所有binlog实体
        for (CanalEntry.Entry entry : message.getEntries()) {
            // 只处理事务型binlog
            if(entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
                    entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            // 获取binlog文件名
            String logfileName = entry.getHeader().getLogfileName();
            // 获取logfile的偏移量
            long logfileOffset = entry.getHeader().getLogfileOffset();
            // 获取sql语句执行时间戳
            long executeTime = entry.getHeader().getExecuteTime();
            // 获取数据库名
            String schemaName = entry.getHeader().getSchemaName();
            // 获取表名
            String tableName = entry.getHeader().getTableName();
            // 获取事件类型 insert/update/delete
            String eventType = entry.getHeader().getEventType().toString().toLowerCase();
            rowDataBuilder.setLogFileName(logfileName);
            rowDataBuilder.setLogFileOffset(logfileOffset);
            rowDataBuilder.setExecuteTime(executeTime);
            rowDataBuilder.setSchemaName(schemaName);
            rowDataBuilder.setTableName(tableName);
            rowDataBuilder.setEventType(eventType);
            // 获取所有行上的变更
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            List<CanalEntry.RowData> columnDataList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : columnDataList) {
                if(eventType.equals("insert") || eventType.equals("update")) {
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        rowDataBuilder.putColumns(column.getName(), column.getValue().toString());
                    }
                }
                else if(eventType.equals("delete")) {
                    for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                        rowDataBuilder.putColumns(column.getName(), column.getValue().toString());
                    }
                }
            }
        }
        return rowDataBuilder.build().toByteArray();
    }
}
