package myflink.stream.task;

import static java.time.LocalDateTime.now;

import java.util.Comparator;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author
 */
public class SocketWindowCountTopN {

  public static void main(String[] args) throws Exception {

    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    String host = parameterTool.get("host", "localhost");
    int port = parameterTool.getInt("port", 9000);

    // 先初始化执行环境
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

    // 输入流
    DataStreamSource<String> socketTextStream = environment.socketTextStream(host, port);

    int topSize = 2;

    // 对输入的数据进行拆分处理
    DataStream<Tuple2<String, Long>> ret = socketTextStream.flatMap(split2Word())
        // 根据tuple2 中的第一个值分组
        .keyBy(0)
        // 设置窗口，每 30s为一个窗口，每5s计算一次
        .timeWindow(Time.seconds(30), Time.seconds(5))
        // 相同字母次数相加
        .reduce(CountReduce())
        // 滑动窗口，每 30s为一个窗口，每5s计算一次
        .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(5)))
        // 对同一个窗口的所有元素排序取前topSize
        .process(new TopN(topSize));

    ret.addSink(new RichSinkFunction<Tuple2<String, Long>>() {

      @Override
      public void invoke(Tuple2<String, Long> value, Context context) {
        System.out.println(now() + " word: " + value.f0 + " count: " + value.f1);
      }
    });

    environment.execute("SocketWindowCountTopN");


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


  static class TopN extends
      ProcessAllWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, TimeWindow> {

    private final int topSize;

    TopN(int topSize) {
      this.topSize = topSize;
    }

    @Override
    public void process(Context context, Iterable<Tuple2<String, Long>> elements,
        Collector<Tuple2<String, Long>> out) throws Exception {

      /*
      1 先创建一颗有序树，
      2 依次往树里面放数据
      3 如果超过topSize 那么就去掉树的最后一个节点
       */

      TreeMap<Long, Tuple2<String, Long>> treeMap = new TreeMap<>(
          new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
              return o2 > o1 ? 1 : -1;
            }
          }
      );

      for (Tuple2<String, Long> element : elements) {

        treeMap.put(element.f1, element);
        if (treeMap.size() > this.topSize) {
          treeMap.pollLastEntry();
        }

      }

      for (Entry<Long, Tuple2<String, Long>> longTuple2Entry : treeMap.entrySet()) {
        out.collect(longTuple2Entry.getValue());
      }

    }
  }

}
