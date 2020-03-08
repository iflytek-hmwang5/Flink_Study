import org.apache.flink.api.scala._

/**
  * Created by hmwang on 2020/3/7.
  * flink批处理实例
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    // 构建flink执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 创建数据集
    val inputPath = "F:\\idea\\flinkws\\FlinkDemo\\src\\main\\resources\\word.txt"

    val data: DataSet[String] = env.readTextFile(inputPath)

    val res: AggregateDataSet[(String, Int)] = data.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)

    res.print()

  }
}
