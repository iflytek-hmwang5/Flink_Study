import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * Created by hmwang on 2020/3/7.
  * flink实时流实例
  */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 构建flink运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度，优先级最高 > 提交时设置 > 配置文件设置
    env.setParallelism(8)
    // 禁用任务合并
    env.disableOperatorChaining()

    //获取socket输入流
    val textDstream: DataStream[String] = env.socketTextStream("bigdata1", 7777)

    val dataStream: DataStream[(String, Int)] =
      textDstream.flatMap(_.split("\\s"))
        .disableChaining()// 单个操作从链中去除
        .startNewChain() // 从这个操作开始重新开始一个链
        .filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)

    dataStream.print()

    env.execute("StreamWordCount")
  }
}
