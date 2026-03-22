use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// This is the event sent from a device to the system.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, Hash, PartialEq)]
pub struct SimpleCall {
    /// The phone is the device identifier.
    pub phone: String,
    /// The triggertime is the event identifier.
    ///
    /// Retried events have the same triggertime as the original event.
    pub triggertime: chrono::DateTime<chrono::Utc>,
}

impl SimpleCall {
    pub fn new_now(phone: String) -> Self {
        Self {
            phone,
            triggertime: chrono::Utc::now(),
        }
    }
}

/// This is the event our system processes.
///
/// It wraps the original event sent from the device, and adds some metadata to it.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, Hash, PartialEq)]
pub struct Event {
    /// The device_id helps us ensure in order delivery of events from the same device.
    pub device_id: String,
    /// The event_id helps us prevent duplicated events.
    ///
    /// It should also impl [`Ord`] and [`Eq`] to ensure that events can be ordered and compared.
    pub event_id: u64,
    /// This is the original event [`SimpleCall`].
    ///
    /// We intentionally erase it, since our system does not care about the content of the event.
    pub original: serde_json::Value,
}

impl From<SimpleCall> for Event {
    fn from(call: SimpleCall) -> Self {
        Self {
            device_id: call.phone.clone(),
            event_id: call.triggertime.timestamp_millis() as u64,
            original: serde_json::to_value(call).expect("Failed to serialize SimpleCall"),
        }
    }
}

impl Event {
    /// Removes the duplicated events, and returns the unique events and the duplicated events separately. The order of the events is preserved.
    pub fn remove_duplication<'a>(
        events: impl Iterator<Item = &'a Event> + 'a,
    ) -> (
        impl Iterator<Item = &'a Event> + 'a,
        impl Iterator<Item = &'a Event> + 'a,
    ) {
        use std::collections::HashSet;

        let mut seen: HashSet<&'a Event> = HashSet::new();
        let mut unique = Vec::new();
        let mut duplicates = Vec::new();

        for e in events {
            if seen.insert(e) {
                unique.push(e);
            } else {
                duplicates.push(e);
            }
        }

        (unique.into_iter(), duplicates.into_iter())
    }

    pub fn validate_order<'a>(
        mut events: impl Iterator<Item = &'a Event> + 'a,
    ) -> Result<(), OrderError> {
        events.try_fold(HashMap::<String, u64>::new(), |mut last, event| {
            last.get(&event.device_id)
                .filter(|&&prev| event.event_id < prev)
                .map(|&prev| OrderError::new(event.device_id.clone(), event.event_id, prev))
                .map_or_else(
                    || {
                        last.insert(event.device_id.clone(), event.event_id);

                        Ok(last)
                    },
                    Err,
                )
        })?;

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Events with id {device_id} are out of order: {first_event_id} < {second_event_id}")]
pub struct OrderError {
    device_id: String,
    first_event_id: u64,
    second_event_id: u64,
}

impl OrderError {
    const fn new(device_id: String, first: u64, second: u64) -> Self {
        Self {
            device_id,
            first_event_id: first,
            second_event_id: second,
        }
    }
}
