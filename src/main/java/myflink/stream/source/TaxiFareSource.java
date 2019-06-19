package myflink.stream.source;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.zip.GZIPInputStream;
import myflink.entity.TaxiFare;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author
 */
public class TaxiFareSource implements SourceFunction<TaxiFare> {

  private GZIPInputStream gzipStream;
  private BufferedReader reader;

  @Override
  public void run(SourceContext<TaxiFare> ctx) throws Exception {

    URL url = TaxiRideSource.class.getClassLoader().getResource("nycTaxiFares.gz");

    gzipStream = new GZIPInputStream(new FileInputStream(url.getPath()));
    reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));

    generateUnorderedStream(ctx);

    this.reader.close();
    this.reader = null;
    this.gzipStream.close();
    this.gzipStream = null;

  }

  private void generateUnorderedStream(SourceContext<TaxiFare> ctx) throws IOException {

    String line;

    while (reader.ready() && (line = reader.readLine()) != null) {

      TaxiFare fare = TaxiFare.fromString(line);
      ctx.collectWithTimestamp(fare, fare.getEventTime());

    }

  }

  @Override
  public void cancel() {

    if (this.reader != null) {
      try {
        this.reader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    if (this.gzipStream != null) {
      try {
        this.gzipStream.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

}
