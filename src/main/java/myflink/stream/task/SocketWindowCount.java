package myflink.stream.task;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author
 */
public class SocketWindowCount {

  public static void main(String[] args) throws Exception {

    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    String host = parameterTool.get("host", "localhost");
    int port = parameterTool.getInt("port", 9000);

    // 先初始化执行环境
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

    // 设置并发度为1， 方便观察输出
    environment.setParallelism(1);

    DataStreamSource<String> socketTextStream = environment.socketTextStream(host, port);

    // 对输入的数据进行拆分处理
    DataStream<Tuple2<String, Long>> reduce = socketTextStream.flatMap(split2Word())
        // 根据tuple2 中的第一个值分组
        .keyBy(0)
        // 设置窗口，每 30s为一个窗口，每5s计算一次
        .timeWindow(Time.seconds(30), Time.seconds(5))
        .reduce(CountReduce());

    reduce.print();

    environment.execute("SocketWindowCount");

  }

  private static ReduceFunction<Tuple2<String, Long>> CountReduce() {
    return new ReduceFunction<Tuple2<String, Long>>() {
      @Override
      public Tuple2<String, Long> reduce(Tuple2<String, Long> value1,
          Tuple2<String, Long> value2)
          throws Exception {
        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
      }
    };
  }

  // 将输入的一行
  private static FlatMapFunction<String, Tuple2<String, Long>> split2Word() {
    return new FlatMapFunction<String, Tuple2<String, Long>>() {
      @Override
      public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {

        String[] words = value.split("\\W");

        for (String word : words) {
          if (word.length() > 0) {
            out.collect(new Tuple2<>(word, 1L));
          }
        }


      }
    };
  }


}
