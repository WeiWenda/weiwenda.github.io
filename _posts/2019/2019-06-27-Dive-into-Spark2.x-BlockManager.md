---
layout: post
category:
tags: []
---
本文只陈述Spark2.x BlockManager的宏观架构，不涉及具体代码，希望能够帮助读者在阅读代码前了解功能类在整个系统中的作用，避免盲人摸象。

总的来讲，Spark有这样几个特点：

1. 可利用内存缓存中间结果的BlockManager
2. 根据RDD lineage实现的DAG调度
3. 依赖HDFS进行的数据持久化
4. 依赖RDD或DataSet强大表达能力所开发的GraphX、MLlib、Streaming、Spark SQL
 
本文将介绍第一个特点 - BlockManager，由于利用内存缓存中间结果比较复杂，MemoryStore的架构将单独介绍。

陈述一、BlockManager在Spark中用来管理RDD cache状态下的内容、Broadcast变量、未开启park.shuffle.service.enabled情况下的Shuffle data文件和index文件。
1. 如果将程序表示为输入+过程（还可能修改公共状态）+输出的话，BlockManager在整个Spark中的作用就是管理中间Stage的输入和输出。而调度系统的作用则是将海量输入切分为单Executor可处理的数据分片（例如一个RDD分区、一个Shuffle过程的reduce端聚集），拼装Stage，分发Task，最后允许中间结果以分布式文件的形式存在。不妨将BlockManger看做是粘接单独运行的Stage的连接件。
![](http://ww1.sinaimg.cn/large/006tNc79ly1g4d5yb6pobj30fv07jq4j.jpg)

陈述二、Driver和Executor都有SparkEnv，SparkEnv中都有BlockManager，BlockManager主要包含以下成员：
1. BlockManagerMaster，BlockManagerMaster持有着指向Driver上BlockManagerMasterEndpoint的RpcEndpointRef
2. BlockManagerSlaveEndpoint，用来接收Driver的数据块指令。
3. BlockTransferService，用来建立stream管道高效传输数据块。
4. ExternalShuffleClient，只有开启spark.shuffle.service.enabled时，才会通过ExternalShuffleClient从外部ShuffleService拉取ShuffleBlock。否则直接利用BlockTransferService拉取ShuffleBlock。

请读者注意，单在数据块管理模块，已经形成了BlockManagerMasterEndpoint和BlockManagerSlaveEndpoint的主从结构。

陈述三、BlockManagerMasterEndpoint管理着所有Block的位置信息、所有BlockManager的数据端口和rpc端口

1. 所述Block的位置信息具体是指该Block目前位于哪些BlockManager
2. 所述BlockManager的数据端口具体是指BlockManager上BlockTransferService所在的端口，任何人只要有BlockTransferService的hostname、port，以及想要获取的数据块BlockId，就可以轻松获取到想要的数据块。目前位置信息直接表示在BlockManagerId中。
3. 所述BlockManager的rpc端口具体是指所在Executor的rpc端口，也就是BlockManagerSlaveEndpoint所在的端口，Driver向这个端口发送rpc请求来指挥Executor的BlockManager及时释放不需要的块。

陈述四、当调用rdd.unpersist后或者ContextCleaner回收rdd时，Driver通知BlockManagerMasterEndpoint RemoveRdd(rddId)，BlockManagerMasterEndpoint随后查看该rdd包含哪些RDDBlock，最后通知每个持有这些RDDBlock的BlockManager释放它们，具体怎么通知见陈述三.3。

陈述五、BlockManager在Executor初始化时会将自己注册到BlockManagerMasterEndpoint中；另外，BlockManager的四大操作putBytes、putIterator、removeBlock、dropFromMemory，都需要将块状态更新到BlockManagerMasterEndpoint，从而保证整个存储系统的状态一致性。
1. BlockManager注册时并不知道自己的rpc端口，Driver侧NettyRpcEndpointRef在RequestMessage反序列化过程中，将当前使用的TransportClient绑定到了NettyRpcEndpointRef，因此才得到了真正的rpc端口信息（见RequestMessage.apply）
![](http://ww1.sinaimg.cn/large/006tNc79ly1g4dha5uvmoj30cv031t8x.jpg)\
图为NettyRpcEndpointRef的反序列化
![](http://ww2.sinaimg.cn/large/006tNc79ly1g4dgid7qgsj30kq02waac.jpg)\
图为NettyRpcEnv的currentClient，重点看注释部分

陈述六、BlockManager在get Block块时，首先从本地尝试获取，失败后rpc咨询BlockManagerMasterEndpoint，哪些BlockManager保存了这个Block，然后按距离远近，优先尝试从较近的BlockManager拉取Block内容，最后由BlockTransferService完成块传输。

## 牛刀小试
为了帮助读者增加自信心，这里利用Spark的BlockManager实现了一个简单的分布式键值存储命令行程序：
```scala
package org.apache.spark

import java.net.InetAddress

import jline.console.ConsoleReader

import org.apache.spark.memory.UnifiedMemoryManager
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.apache.spark.storage._
// mode port [master hostname] [master rpcport]
class Endpoint(args: Array[String]) {
  val clientMode = args(0).equals("client")
  val hostname = InetAddress.getLocalHost().getHostName
  val rpcPort = args(1).toInt
  val conf = new SparkConf().set("spark.local.dir", "/Users/weiwenda/tmp").set("spark.app.id", "useless")
  // 照搬SparkEnv.create的部分代码
  val securityManager = new SecurityManager(conf, None)
  // client模式不启动TransportServer，server模式在指定端口启动TransportServer
  val rpcEnv = RpcEnv.create(args(0), hostname, hostname, rpcPort, conf, securityManager, 1, clientMode)
  val listenerBus = new LiveListenerBus(conf)
  val blockTransferService =
    new NettyBlockTransferService(conf, securityManager, hostname, hostname, 0, 1)
  val blockManagerMaster = new BlockManagerMaster(registerOrLookupEndpoint(
    BlockManagerMaster.DRIVER_ENDPOINT_NAME,
    new BlockManagerMasterEndpoint(rpcEnv, false, conf, listenerBus)),
    conf, !clientMode)
  val serializer = new org.apache.spark.serializer.KryoSerializer(conf)
  val serializerManager = new SerializerManager(serializer, conf, None)
  val memoryManager = UnifiedMemoryManager(conf, 1)
  // 创建一个简陋版的BlockManager，由于不涉及shuffle部分，直接将mapOutputTracker和shuffleManager置空也可以
  val blockManager = new BlockManager(args(0), rpcEnv, blockManagerMaster,
    serializerManager, conf, memoryManager, null, null,
    blockTransferService, securityManager, 1)
  blockManager.initialize("useless")

  def set(key: String, value: String): Unit = {
    blockManager.putSingle(TestBlockId(key), value, MEMORY_AND_DISK)
  }
  def get(key: String): Option[String] = {
    val result = blockManager.get[String](TestBlockId(key))
    if (result.isEmpty) {
      println(s"Can not Found block ${TestBlockId(key)} anywhere")
    }
    result.map(_.data.next().asInstanceOf[String])
  }

  def registerOrLookupEndpoint(name: String, endpointCreator: => RpcEndpoint): RpcEndpointRef = {
    if (!clientMode) {
      rpcEnv.setupEndpoint(name, endpointCreator)
    } else {
      rpcEnv.setupEndpointRef(RpcAddress(args(2), args(3).toInt), name)
    }
  }
}
object BlockManagerTrial {
  def main(args: Array[String]): Unit = {
    val endpoint = new Endpoint(args)
    val reader = new ConsoleReader()
    var prompt = "rpctest"
    var promptSpace = "       "
    var currentPrompt = prompt
    var line = reader.readLine(currentPrompt + "> ")
    var prefix = ""
    while (line != null) {
      if (prefix.nonEmpty) {
        prefix += ' '
      }
      if (line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
        line = prefix + line
        //目前允许用户使用两种命令：
        // 1. set key value
        // 2. get key
        val commands = line.replace(";", "").trim.split("\\s+")
        if (commands.size == 3 && commands(0).equals("set")){
          endpoint.set(commands(1), commands(2))
        }
        if (commands.size == 2 && commands(0).equals("get")) {
          endpoint.get(commands(1)).foreach(println)
        }
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
            <artifactId>spark-core_2.11</artifactId>
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
java -cp ./spark-rpc-test-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.spark.BlockManagerTrial server 10000
```
找另一台机器作为客户端（另开一个terminal也可以），以client模式启动，并连接TransportServer
```
java -cp ./spark-rpc-test-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.spark.BlockManagerTrial client anynum hostnameOfA 10000
```
在任意一台机器输入`set key value`之后，在另一台机器通过`get key`都可以获取到所设置的`value`。