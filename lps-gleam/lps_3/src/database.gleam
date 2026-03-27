import event
import gleam/int
import gleam/string

pub type Event {
  Event(id: String, data: String)
}

pub fn from_event(event: event.Event) -> Event {
  let event.Event(device_id, event_id) = event

  Event(
    id: device_id <> "-" <> int.to_string(event_id),
    data: string.inspect(event),
  )
}
