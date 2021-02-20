package com.itheima.canal_demo;

import com.google.protobuf.InvalidProtocolBufferException;
import com.itheima.protobuf.DemoModel;

/**
 * 使用protobuf生成的对象，分别给对象赋值，然后获取值
 */
public class ProtoBufDemo {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //实例化User对象
        DemoModel.User.Builder builder = DemoModel.User.newBuilder();
        //给User对象赋值
        builder.setId(1);
        builder.setName("张三");
        builder.setSex("男");

        //获取User的值
        DemoModel.User userBuild = builder.build();//.toByteArray()
        System.out.println(userBuild.getName());

        byte[] bytes = builder.build().toByteArray();
        for(byte b: bytes){
            System.out.print(b);
        }

        //将二进制转换成对象
        // 可以将kakfa中的数据序列化成为对应的系列化的对象的。
        DemoModel.User user = DemoModel.User.parseFrom(bytes);
        System.out.println(user.getName());
    }
}
