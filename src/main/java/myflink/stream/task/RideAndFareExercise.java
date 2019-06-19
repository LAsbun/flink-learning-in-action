package myflink.stream.task;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.TreeMap;
import myflink.entity.TaxiFare;
import myflink.entity.TaxiRide;
import myflink.stream.source.TaxiFareSource;
import myflink.stream.source.TaxiRideSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author
 */
public class RideAndFareExercise {

  static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {
  };
  static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {
  };

  public static void main(String[] args) throws Exception {

    // 初始化enviorment
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

    // 设置事件事件
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // 读取输入流

    DataStreamSource<TaxiFare> taxiFareDataStreamSource = environment
        .addSource(new TaxiFareSource());

    DataStream<TaxiFare> taxiFare = taxiFareDataStreamSource
        .keyBy(new KeySelector<TaxiFare, Long>() {
          @Override
          public Long getKey(TaxiFare value) throws Exception {
            return value.getRideId();
          }
        });

//    taxiFareDataStreamSource.print();
    DataStreamSource<TaxiRide> taxiRideDataStreamSource = environment
        .addSource(new TaxiRideSource());

//    taxiRideDataStreamSource.print();
    // 对source 过滤掉rided % 1000 == 0, 模拟现实世界丢失数据
    DataStream<TaxiRide> taxiRide = taxiRideDataStreamSource.filter(new FilterFunction<TaxiRide>() {
      @Override
      public boolean filter(TaxiRide value) throws Exception {
        return value.isStart && value.getRideId() % 1000 != 0;
      }
    })
        .keyBy(new KeySelector<TaxiRide, Long>() {
          @Override
          public Long getKey(TaxiRide value) throws Exception {
            return value.getRideId();
          }
        });

    // join 将两个输入流以rideId为key， 合并
    SingleOutputStreamOperator<Tuple2<TaxiFare, TaxiRide>> process = taxiFare.connect(taxiRide)
        .process(new ConnectProcess());

    // 设置窗口
    SingleOutputStreamOperator<Tuple3<Long, Float, Timestamp>> aggregate = process
        // 先将taxiFare 筛选出来，因为是要统计topN taxiFare
        .flatMap(new FlatMapFunction<Tuple2<TaxiFare, TaxiRide>, TaxiFare>() {
          @Override
          public void flatMap(Tuple2<TaxiFare, TaxiRide> value, Collector<TaxiFare> out)
              throws Exception {
            out.collect(value.f0);
          }
        })
        // 因为是时间递增，所以watermark 很简单
        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TaxiFare>() {
          @Override
          public long extractAscendingTimestamp(TaxiFare element) {
//            System.out.println(element.getEventTime());
            return element.getEventTime();
          }
        })
        // 根据 driverId 分组
        .keyBy(new KeySelector<TaxiFare, Long>() {
          @Override
          public Long getKey(TaxiFare value) throws Exception {
            return value.getDriverId();
          }
        })
        // 设置时间窗口，每30min 计算一次最近1个小时的内driverId的总收入
        .timeWindow(Time.hours(1), Time.minutes(30))
        // 这个是累加函数,调用aggregate 结果会计算出同一个窗口中，每个driverId的收入总值
        .aggregate(getAggregateFunction(),
            // 这个windowFunc 是格式化输出
            new WindowFunction<Float, Tuple3<Long, Float, Timestamp>, Long, TimeWindow>() {
              @Override
              public void apply(Long driverId, TimeWindow window, Iterable<Float> input,
                  Collector<Tuple3<Long, Float, Timestamp>> out)
                  throws Exception {
                Float next = input.iterator().next();
                out.collect(new Tuple3(driverId, next, new Timestamp(window.getEnd())));
              }
            });

    // topSize N
    int topSize = 3;

    aggregate
        // 根据时间进行分窗口
        .keyBy(2)
        .timeWindow(Time.hours(1), Time.minutes(30))
        .process(new topN(topSize)).print();
    environment.execute("RideAndFareExercise");

  }

  // 累加函数
  private static AggregateFunction<TaxiFare, Float, Float> getAggregateFunction() {
    return new AggregateFunction<TaxiFare, Float, Float>() {

      @Override
      public Float createAccumulator() {
        return 0f;
      }

      @Override
      public Float add(TaxiFare value, Float accumulator) {
        return value.totalFare + accumulator;
      }

      @Override
      public Float getResult(Float accumulator) {
        return accumulator;
      }

      @Override
      public Float merge(Float a, Float b) {
        return a + b;
      }
    };
  }

  // 连接函数
  private static class ConnectProcess extends
      CoProcessFunction<TaxiFare, TaxiRide, Tuple2<TaxiFare, TaxiRide>> {

    // 设置两个state
    private ValueState<TaxiFare> fareListState;

    private ValueState<TaxiRide> rideListState;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);

      fareListState = getRuntimeContext()
          .getState(new ValueStateDescriptor<TaxiFare>("taxifare list state", TaxiFare.class));
      rideListState = getRuntimeContext()
          .getState(new ValueStateDescriptor<TaxiRide>("taxiride list state", TaxiRide.class));

    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx,
        Collector<Tuple2<TaxiFare, TaxiRide>> out)
        throws Exception {
      super.onTimer(timestamp, ctx, out);

      if (fareListState.value() != null) {
        ctx.output(unmatchedFares, fareListState.value());
        fareListState.clear();
      }
      if (rideListState.value() != null) {
        ctx.output(unmatchedRides, rideListState.value());
        rideListState.clear();
      }

    }

    @Override
    public void processElement1(TaxiFare value, Context ctx,
        Collector<Tuple2<TaxiFare, TaxiRide>> out) throws Exception {

      TaxiRide ride = rideListState.value();
      if (ride != null) {
        out.collect(new Tuple2<>(value, ride));
        rideListState.clear();
      } else {
        fareListState.update(value);
        ctx.timerService().registerEventTimeTimer(value.getEventTime());
      }

    }

    @Override
    public void processElement2(TaxiRide value, Context ctx,
        Collector<Tuple2<TaxiFare, TaxiRide>> out) throws Exception {

      TaxiFare fare = fareListState.value();
      if (fare != null) {
        out.collect(new Tuple2<>(fare, value));
        fareListState.clear();
      } else {
        rideListState.update(value);
        ctx.timerService().registerEventTimeTimer(value.getEventTime());
      }
    }
  }

  static class topN extends
      ProcessWindowFunction<Tuple3<Long, Float, Timestamp>, String, Tuple, TimeWindow> {

    private final int topSize;

    public topN(int topSize) {
      this.topSize = topSize;
    }


    @Override
    public void process(Tuple tuple, Context context,
        Iterable<Tuple3<Long, Float, Timestamp>> elements, Collector<String> out) throws Exception {

      TreeMap<Float, Tuple3<Long, Float, Timestamp>> treeMap = new TreeMap<>(
          new Comparator<Float>() {
            @Override
            public int compare(Float o1, Float o2) {
              return o1.equals(o2) ? 0 : (o1 < o2 ? 1 : -1);
            }
          }
      );

      for (Tuple3<Long, Float, Timestamp> element : elements) {
        treeMap.put(element.f1, element);
        if (treeMap.size() > this.topSize) {
          treeMap.pollLastEntry();
        }
      }
      System.out.println(" key: " + ((Tuple1) tuple).f0);

      for (Entry<Float, Tuple3<Long, Float, Timestamp>> floatTuple3Entry : treeMap.entrySet()) {

        String s = "driverId: " + floatTuple3Entry.getValue().f0 + " total：" + floatTuple3Entry
            .getValue().f1 + " at " + ((Tuple1) tuple).f0;
        out.collect(s);

      }


    }
  }
}
