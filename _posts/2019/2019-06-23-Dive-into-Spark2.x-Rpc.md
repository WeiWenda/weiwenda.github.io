---
layout: post
category:
tags: []
---
本文只陈述Spark2.x Rpc的宏观架构，不涉及具体代码，希望能够帮助读者在阅读代码前了解功能类在整个系统中的作用，避免盲人摸象。

陈述一、用netty实现的rpc系统，在服务端一般会启动io.netty.bootstrap.ServerBootstrap，在客户端一般会启动Bootstrap，rpc系统的具体逻辑一般实现在ChannelInboundHandlerAdapter的channelRead中

1. 所述服务端，即Server，在Master-Slave分布式系统中的通常对应Master角色；所述客户端，即Client，通常对应Slave角色。但Master-Slave分布式系统进行任务分发、任务中止、数据块删除备份的过程中，Slave将充当服务端，Master相反充当客户端。
2. 1中所述Master-Slave分布式系统，具体到Spark，由Driver充当Master角色，由Executor充当Slave角色
3. 2中所述Driver在client部署模式下，运行在提交spark程序的机器上，也就是大家俗称和使用的客户端，注意这里的客户端和陈述一中的客户端不是一个概念；在cluster部署模式下，运行在Worker（spark standalone）或NodeManger（spark on yarn）所在的机器上，也就是大家俗称的集群。而Executor在client和cluster部署模式下，均运行在集群上。

陈述二、Driver和Executor都有SparkEnv，SparkEnv中都有RpcEnv，RpcEnv中都有TransportContext和TransportClientFactory，但只有Driver的RpcEnv中创建了TransportServer，正是TransportServer启动了ServerBootstrap。Executor为了注册到Driver，需要与Driver的TransportServer建立rpc连接，这时就会通过RpcEnv中的TransportClientFactory创建TransportClient，正是TransportClient启动了Bootstrap。

1. Spark中为了节省rpc连接量，只在Driver端启动了ServerBootstrap，当Executor注册到Driver时，所建立的Channel就成了Executor与Driver、以及Driver与Executor交互的唯一通道，这也是Executor不需要创建TransportServer的原因。但这样做的缺点也很明显，所建立的Channel一旦中断，Driver就会认为Executor不再可用，随即尝试rpc关闭该Executor，并通知Scheduler移除该Executor的注册信息，不再使用它。

陈述三、Spark实现的ChannelInboundHandlerAdapter叫做TransportChannelHandler，它的channelRead在服务端用来接收rpc请求，在客户端用来接收rpc调用的返回值。

1. Spark中接收rpc请求后的响应工作被TransportRequestHandler委托给了RpcHandler，比较常见的RpcHandler是NettyRpcHandler，它通过Dispatcher分发器实现了多组命名空间不同且相互隔离的rpc服务，每个独立的rpc服务由一个RpcEndpoint负责
2. Spark中接收rpc调用返回值的工作由TransportResponseHandler完成，实际上只是回调了RpcResponseCallback.onSuccess，最常见的NettyRpcEnv.ask中的RpcResponseCallback.onSuccess只是原封不动的返回 Array[Byte]，客户端需要按照约定反序列化
3. 下表列出了Driver、Executor、AM的rpc服务及相应的命名空间名称


Driver | Executor | AM | 功能
---|---| -- | --
BlockManagerMasterEndpoint </br>`BlockManagerMaster` | BlockManagerSlaveEndpoint</br> `BlockManagerEndpoint` | | 数据块管理（Executor的BlockManager中的BlockTransferService会启动TransportServer，数据块传输与数据块管理不共用端口）
DriverEndpoint </br>`CoarseGrainedScheduler` | CoarseGrainedExecutorBackend </br>`Executor` | | 任务调度
HeartbeatReceiver | |  |任务调度
YarnSchedulerEndpoint </br>`YarnScheduler` | | AMEndpoint</br> `YarnAM` | 动态调度
MapOutputTrackerMasterEndpoint</br> `MapOutputTracker ` | | |shuffle执行
OutputCommitCoordinatorEndpoint</br> `OutputCommitCoordinator`| | 

陈述四、一次rpc调用，由RpcEndpointRef.ask或.send发起，经过NettyRpcEnv.ask或.send，最终由TransportClient.sendRpc或send发出。响应逻辑见陈述三.1，响应接收逻辑见陈述三.2

1. RpcEndpointRef可以认为是host、port、rpc服务命名空间名称构成的三元组，TransportClientFactory根据这些信息就可以构建出相应的TransportClient（体现在Executor注册时），或者根据这些信息找到已经建立好的TransportClient（体现在Driver任务分发、Executor心跳等）。

## 牛刀小试
为了帮助读者增加自信心，这里利用Spark的rpc基础类实现了一个简单的带聊天功能的命令行程序：
```scala
package com.qihoo.xitong

import java.net.{InetAddress, InetSocketAddress}
import java.nio.ByteBuffer

import jline.console.ConsoleReader

import org.apache.spark.network.TransportContext
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.util.{JavaUtils, MapConfigProvider, TransportConf}
//根据参数充当服务端或客户端
//服务端的参数格式: client serverhost serverport
//客户端的参数格式: server serverport
class Endpoint(args: Array[String]) {
  val clientMode = args(0).equals("client")
  //初始化块与NettyRpcEnv的代码较为相似
  val transportConf = new TransportConf("rpctest", MapConfigProvider.EMPTY)
  var client: Option[TransportClient] = Option.empty
  val context = new TransportContext(transportConf, new RepeatRpcHandler(
    client =>
    //服务端在客户端连接时记下客户端的地址
      if(this.client.isEmpty) this.client = Option(client)))
  if (clientMode) {
  //客户端初始化时直接连接服务端
    client = Option(context.createClientFactory().createClient(args(1), args(2).toInt));
  } else {
  //服务端则启动TransportServer等待客户端连接
    context.createServer(args(1).toInt, new java.util.ArrayList())
  }
  def send(message: ByteBuffer): Unit = {
  //将消息以OneWayMessage的方式发出
    client.foreach(_.send(message))
  }
}
object Main {
  def main(args: Array[String]): Unit = {
    val endpoint = new Endpoint(args)
    //下面照搬了SparkSQLCLIDriver的repl逻辑
    val reader = new ConsoleReader()
    var prompt = "rpctest"
    var promptSpace = "       "
    var currentPrompt = prompt
    var line = reader.readLine(currentPrompt + "> ")
    var prefix = ""
    while (line != null) {
      if (prefix.nonEmpty) {
        prefix += '\n'
      }
      if (line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
        line = prefix + line
        endpoint.send(JavaUtils.stringToBytes(line))
        prefix = ""
        currentPrompt = prompt
      } else {
        prefix = prefix + line
        currentPrompt = promptSpace
      }
      line = reader.readLine(currentPrompt + "> ")
    }
  }
}
//自制的简易版RpcHandler，只负责打印收到的消息
//如果是服务端，还可能记录客户端的地址
class RepeatRpcHandler(onReceive: TransportClient => Unit) extends RpcHandler {
  private var streamManager = new OneForOneStreamManager

  override def receive(client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback): Unit = {
    println(s"\rreceive message send from ${client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]}")
    println(s"messge content: ${JavaUtils.bytesToString(message)}")
    onReceive(client)
    Thread.sleep(3000)
    callback.onSuccess(message)
  }

  override def getStreamManager: StreamManager = streamManager
}
```
上述代码对应的pom.xml文件：
```
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.qihoo.xitong</groupId>
    <artifactId>spark-rpc-test</artifactId>
    <version>1.0-SNAPSHOT</version>
    <dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-network-common_2.11</artifactId>
            <version>2.3.0</version>
        </dependency>
        <dependency>
            <groupId>jline</groupId>
            <artifactId>jline</artifactId>
            <version>2.12.1</version>
        </dependency>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>2.11.8</version>
        </dependency>
    </dependencies>
    <build>
        <sourceDirectory>src/main/scala</sourceDirectory>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <configuration>
                    <descriptorRefs>
                        <descriptorRef>jar-with-dependencies</descriptorRef>
                    </descriptorRefs>
                </configuration>
                <executions>
                    <execution>
                        <id>dist</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.2</version>
                <executions>
                    <execution>
                        <id>scala-compile-first</id>
                        <goals>
                            <goal>compile</goal>
                        </goals>
                    </execution>
                    <execution>
                        <id>scala-test-compile-first</id>
                        <goals>
                            <goal>testCompile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```
找一台机器A作为服务端，在10000端口上启动TransportServer
```
java -cp ./spark-rpc-test-1.0-SNAPSHOT-jar-with-dependencies.jar com.qihoo.xitong.Main server 10000
```
找另一台机器作为客户端（另开一个terminal也可以），以client模式启动，并连接TransportServer
```
java -cp ./spark-rpc-test-1.0-SNAPSHOT-jar-with-dependencies.jar com.qihoo.xitong.Main client hostnameOfA 10000
```
然后就可以自己跟自己玩一天了。。
