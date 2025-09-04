package dev.qg.loomer.adls;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/** Utility to compute time buckets for ready steps. */
public final class Buckets {
  private Buckets() {}

  public static String minuteShard(Instant due, String stepId) {
    ZonedDateTime z = due.atZone(ZoneOffset.UTC);
    int shard = Math.floorMod(stepId.hashCode(), 64);
    return String.format("%04d/%02d/%02d/%02d/%02d/%02d",
        z.getYear(), z.getMonthValue(), z.getDayOfMonth(),
        z.getHour(), z.getMinute(), shard);
  }
}
