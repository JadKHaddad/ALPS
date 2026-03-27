import call
import gleam/time/timestamp

/// We save the events in the database to eliminate duplicates
/// 
/// We create an Event from a SimpleCall using from_call
pub type Event {
  Event(device_id: String, event_id: Int)
}

pub fn from_call(call: call.SimpleCall) -> Result(Event, String) {
  let call.SimpleCall(phone, triggertime, _, _) = call

  case utc_string_to_millis(triggertime) {
    Error(_) -> Error("Invalid datetime")
    Ok(millis) -> {
      Ok(Event(device_id: phone, event_id: millis))
    }
  }
}

fn utc_string_to_millis(value: String) -> Result(Int, String) {
  case timestamp.parse_rfc3339(value) {
    Error(_) -> Error("Invalid RFC3339 timestamp")

    Ok(ts) -> {
      let #(seconds, nanoseconds) =
        timestamp.to_unix_seconds_and_nanoseconds(ts)

      Ok(seconds * 1000 + nanoseconds / 1_000_000)
    }
  }
}
