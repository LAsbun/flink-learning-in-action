package myflink.entity;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import lombok.Data;

/**
 * @author
 */
@Data
public class TaxiFare {

  private static transient DateTimeFormatter timeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US);


  public long rideId;
  public long taxiId;
  public long driverId;
  public LocalDateTime startTime;
  public String paymentType;
  public float tip;
  public float tolls;
  public float totalFare;

  public static TaxiFare fromString(String line) {

    String[] tokens = line.split(",");
    if (tokens.length != 8) {
      throw new RuntimeException("Invalid record: " + line);
    }

    TaxiFare fare = new TaxiFare();

    try {
      fare.rideId = Long.parseLong(tokens[0]);
      fare.taxiId = Long.parseLong(tokens[1]);
      fare.driverId = Long.parseLong(tokens[2]);
      fare.startTime = LocalDateTime.parse(tokens[3], timeFormatter);
      fare.paymentType = tokens[4];
      fare.tip = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
      fare.tolls = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0f;
      fare.totalFare = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0f;

    } catch (NumberFormatException nfe) {
      throw new RuntimeException("Invalid record: " + line, nfe);
    }

    return fare;

  }

  public long getEventTime() {
    return this.startTime.toEpochSecond(ZoneOffset.UTC) * 1000;
  }
}
