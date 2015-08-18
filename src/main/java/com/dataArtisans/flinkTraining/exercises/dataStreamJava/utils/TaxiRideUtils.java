package com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;

public class TaxiRideUtils {

  private static transient DateTimeFormatter timeFormatter =
          DateTimeFormat.forPattern("yyyy-MM-DD HH:mm:ss").withLocale(Locale.US).withZoneUTC();

  /** Deserialize a string to a TaxiRide object.
   *
   * Target string format:
   * tripId,time,event[START|END],startLon,startLat,destLon,destLat,passengerCnt,tripDistance
   *
   * @param line String to deserialize
   * @return TaxiRide
   */
  public static TaxiRide deserialize(String line) {
    String[] tokens = line.split(",");
    if (tokens.length != 9) {
      throw new RuntimeException("Invalid record: " + line);
    }

    TaxiRide ride = new TaxiRide();

    try {
      ride.rideId = Long.parseLong(tokens[0]);
      ride.time = DateTime.parse(tokens[1], timeFormatter);
      ride.startLon = tokens[3].length() > 0 ? Float.parseFloat(tokens[3]) : 0.0f;
      ride.startLat = tokens[4].length() > 0 ? Float.parseFloat(tokens[4]) : 0.0f;
      ride.endLon = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
      ride.endLat = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0f;
      ride.passengerCnt = Short.parseShort(tokens[7]);
      ride.travelDistance = tokens[8].length() > 0 ? Float.parseFloat(tokens[8]) : 0.0f;

      if (tokens[2].equals("START")) {
        ride.event = TaxiRide.Event.START;
      } else if (tokens[2].equals("END")) {
        ride.event = TaxiRide.Event.END;
      } else {
        throw new RuntimeException("Invalid record: " + line);
      }

    } catch (NumberFormatException nfe) {
      throw new RuntimeException("Invalid record: " + line, nfe);
    }

    return ride;
  }

}
