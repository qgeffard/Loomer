package dev.qg.loomer.core;

/** Thrown when a run is cancelled. */
public class CancelledException extends Exception {
  public CancelledException(String msg) { super(msg); }
}
