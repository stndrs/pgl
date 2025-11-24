import gleam/int
import gleam/time/calendar
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}
import pgl/internal

// Interval

pub opaque type Interval {
  Interval(dur: Duration)
}

pub fn from_duration(dur: Duration) -> Interval {
  Interval(dur:)
}

pub fn from_time_of_day(time: calendar.TimeOfDay) -> Interval {
  hours(time.hours)
  |> add(minutes(time.minutes))
  |> add(seconds(time.seconds))
  |> add(nanoseconds(time.nanoseconds))
}

pub fn add(left: Interval, right: Interval) -> Interval {
  left.dur |> duration.add(right.dur) |> from_duration
}

pub fn add_to(interval: Interval, ts: Timestamp) -> Timestamp {
  timestamp.add(ts, interval.dur)
}

pub fn nanoseconds(count: Int) -> Interval {
  count
  |> duration.nanoseconds
  |> from_duration
}

pub fn microseconds(count: Int) -> Interval {
  count
  |> int.multiply(internal.nsecs_per_usec)
  |> nanoseconds
}

pub fn seconds(count: Int) -> Interval {
  count
  |> duration.seconds
  |> from_duration
}

pub fn minutes(count: Int) -> Interval {
  count
  |> duration.minutes
  |> from_duration
}

pub fn hours(count: Int) -> Interval {
  count
  |> duration.hours
  |> from_duration
}

pub fn days(count: Int) -> Interval {
  count
  |> int.multiply(24)
  |> hours
}

pub fn to_microseconds(interval: Interval) -> Int {
  let #(seconds, nanoseconds) =
    duration.to_seconds_and_nanoseconds(interval.dur)

  { seconds * internal.usecs_per_sec }
  + { nanoseconds / internal.nsecs_per_usec }
}

pub fn to_duration(interval: Interval) -> Duration {
  interval.dur
}

pub fn unix_seconds_before_postgres_epoch() -> Interval {
  internal.gs_to_unix_epoch
  |> int.subtract(internal.postgres_gs_epoch)
  |> seconds
}
