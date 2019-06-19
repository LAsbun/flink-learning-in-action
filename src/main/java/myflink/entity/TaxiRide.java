package myflink.entity;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import lombok.Data;

/**
 * @author
 */
@Data
public class TaxiRide {

  private static transient DateTimeFormatter timeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US);


  public long rideId;
  public boolean isStart;
  public LocalDateTime startTime;
  public LocalDateTime endTime;
  public float startLon;
  public float startLat;
  public float endLon;
  public float endLat;
  public short passengerCnt;
  public long taxiId;
  public long driverId;


  public static TaxiRide fromString(String line) {

    String[] tokens = line.split(",");
    if (tokens.length != 11) {
      throw new RuntimeException("Invalid record: " + line);
    }

    TaxiRide ride = new TaxiRide();

    try {
      ride.rideId = Long.parseLong(tokens[0]);

      switch (tokens[1]) {
        case "START":
          ride.isStart = true;
          ride.startTime = LocalDateTime.parse(tokens[2], timeFormatter);
          ride.endTime = LocalDateTime.parse(tokens[3], timeFormatter);
          break;
        case "END":
          ride.isStart = false;
          ride.endTime = LocalDateTime.parse(tokens[2], timeFormatter);
          ride.startTime = LocalDateTime.parse(tokens[3], timeFormatter);
          break;
        default:
          throw new RuntimeException("Invalid record: " + line);
      }

      ride.startLon = tokens[4].length() > 0 ? Float.parseFloat(tokens[4]) : 0.0f;
      ride.startLat = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
      ride.endLon = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0f;
      ride.endLat = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0f;
      ride.passengerCnt = Short.parseShort(tokens[8]);
      ride.taxiId = Long.parseLong(tokens[9]);
      ride.driverId = Long.parseLong(tokens[10]);

    } catch (NumberFormatException nfe) {
      throw new RuntimeException("Invalid record: " + line, nfe);
    }

    return ride;
  }


  // getEventTime 事件事件
  public long getEventTime() {

    if (this.isStart) {
      return this.startTime.getNano() / 1000;
    } else {
      return this.endTime.getNano() / 1000;
    }

  }
}
