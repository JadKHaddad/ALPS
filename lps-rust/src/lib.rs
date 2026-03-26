use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

/// This is the event sent from a device to the system.
#[derive(Debug, Clone, Serialize, Eq, Hash, PartialEq)]
pub struct SimpleCall {
    /// The phone is the device identifier.
    pub phone: String,
    /// The triggertime is the event identifier.
    ///
    /// Retried events have the same triggertime as the original event.
    pub triggertime: chrono::DateTime<chrono::Utc>,
    /// Not used in our system.
    #[serde(default)]
    branch: String,
    /// Not used in our system.
    #[serde(default)]
    headnumber: String,
}

impl SimpleCall {
    pub fn new_now(phone: String) -> Self {
        Self {
            phone,
            triggertime: chrono::Utc::now(),
            branch: String::from("Branch"),
            headnumber: String::from("Headnumber"),
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

    /// Validates that all events in `should` are present in `actual`. The order of the events is not considered.
    pub fn validate_completeness<'a>(
        should: impl Iterator<Item = &'a Event>,
        actual: impl Iterator<Item = &'a Event>,
    ) -> Result<(), CompletenessError> {
        let actual_set: HashSet<_> = actual.collect();

        let missing = should
            .filter(|e| !actual_set.contains(e))
            .cloned()
            .collect::<Vec<_>>();

        if missing.is_empty() {
            Ok(())
        } else {
            Err(CompletenessError { missing })
        }
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

#[derive(Debug, thiserror::Error)]
#[error("Missing events: {missing:?}")]
pub struct CompletenessError {
    missing: Vec<Event>,
}

/// Default value for failure rate used in servers and proxies.
pub fn default_failure_rate() -> f64 {
    0.1
}

pub fn deserialize_failure_rate<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let rate = f64::deserialize(deserializer)?;

    if !(0.0..=1.0).contains(&rate) {
        return Err(serde::de::Error::custom(format!(
            "failure rate must be between 0 and 1, got {rate}"
        )));
    }

    Ok(rate)
}

pub async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C signal handler");

        tracing::info!("CTRL+C received");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM signal handler")
            .recv()
            .await;

        tracing::info!("SIGTERM received");
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("Shutting down");
}

/// Helpers to deserialize [`SimpleCall`] from different formats.
///
/// - Correct json object with the correct fields.
/// - Json object with a `body` field that contains the correct json object as a string.
/// - A string that contains a json object with a `body` field that contains the correct json object as a string.
const _: () = {
    use serde::{Deserialize, Deserializer, de};

    #[derive(Debug, Deserialize)]
    struct SimpleCallHelper {
        phone: String,
        triggertime: chrono::DateTime<chrono::Utc>,

        #[serde(default)]
        branch: String,

        #[serde(default)]
        headnumber: String,
    }

    #[derive(Debug, Deserialize)]
    struct BodyWrapper {
        body: String,
    }

    impl<'de> Deserialize<'de> for SimpleCall {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let value = serde_json::Value::deserialize(deserializer)?;

            tracing::debug!(?value, "Deserializing SimpleCall from JSON value");

            parse_simplecall_value(value).map_err(de::Error::custom)
        }
    }

    fn parse_simplecall_value(value: serde_json::Value) -> Result<SimpleCall, String> {
        // Case 1: direct object
        if let Ok(data) = serde_json::from_value::<SimpleCallHelper>(value.clone()) {
            return Ok(SimpleCall {
                phone: data.phone,
                triggertime: data.triggertime,
                branch: data.branch,
                headnumber: data.headnumber,
            });
        }

        // Case 2: wrapper object with body
        if let Ok(wrapper) = serde_json::from_value::<BodyWrapper>(value.clone()) {
            let data: SimpleCallHelper =
                serde_json::from_str(&wrapper.body).map_err(|e| e.to_string())?;

            return Ok(SimpleCall {
                phone: data.phone,
                triggertime: data.triggertime,
                branch: data.branch,
                headnumber: data.headnumber,
            });
        }

        // Case 3: top-level string containing wrapper object
        if let serde_json::Value::String(s) = value {
            let nested: serde_json::Value = serde_json::from_str(&s).map_err(|e| e.to_string())?;

            return parse_simplecall_value(nested);
        }

        Err("invalid SimpleCall format".into())
    }
};

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Timelike, Utc};

    #[test]
    fn deserialize_direct_simplecall() {
        let json = r#"
        {
            "phone": "123456",
            "branch": "A",
            "headnumber": "999",
            "triggertime": "2026-03-26T10:30:00Z"
        }
        "#;

        let call: SimpleCall = serde_json::from_str(json).unwrap();

        assert_eq!(call.phone, "123456");
        assert_eq!(call.branch, "A");
        assert_eq!(call.headnumber, "999");
        assert_eq!(
            call.triggertime,
            Utc.with_ymd_and_hms(2026, 3, 26, 10, 30, 0).unwrap()
        );
    }

    #[test]
    fn deserialize_wrapped_body_simplecall() {
        let json = r#"
        {
            "body": "{\"phone\":\"123456\",\"branch\":\"A\",\"headnumber\":\"999\",\"triggertime\":\"2026-03-26T10:30:00Z\"}"
        }
        "#;

        let call: SimpleCall = serde_json::from_str(json).unwrap();

        assert_eq!(call.phone, "123456");
        assert_eq!(call.branch, "A");
        assert_eq!(call.headnumber, "999");
        assert_eq!(
            call.triggertime,
            Utc.with_ymd_and_hms(2026, 3, 26, 10, 30, 0).unwrap()
        );
    }

    #[test]
    fn deserialize_string_wrapper_simplecall() {
        let json = r#"
    "{\"id\":\"9\",\"body\":\"{\\\"phone\\\":\\\"9\\\",\\\"branch\\\":\\\"Branch\\\",\\\"headnumber\\\":\\\"Headnumber\\\",\\\"triggertime\\\":\\\"2026-03-26T09:49:37.044594Z\\\"}\",\"request_id\":\"device-9-event-0-try-1\"}"
    "#;

        let call: SimpleCall = serde_json::from_str(json).unwrap();

        assert_eq!(call.phone, "9");
        assert_eq!(call.branch, "Branch");
        assert_eq!(call.headnumber, "Headnumber");
        assert_eq!(
            call.triggertime,
            Utc.with_ymd_and_hms(2026, 3, 26, 9, 49, 37)
                .unwrap()
                .with_nanosecond(44_594_000)
                .unwrap()
        );
    }
}
