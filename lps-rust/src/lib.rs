use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    time::Duration,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub mod consumer;
pub mod device;
pub mod producer;
pub mod server;
pub mod server_proxy;
pub mod simulation;
pub mod tcp_proxy;
pub mod validator;

/// This is the event sent from a device to the system.
#[derive(Debug, Clone, Serialize, Eq, Hash, PartialEq)]
pub struct SimpleCall {
    /// The phone is the device identifier.
    pub phone: String,
    /// The triggertime is the event identifier.
    ///
    /// Retried events have the same triggertime as the original event.
    pub triggertime: DateTime<Utc>,
    /// Not used in our system.
    #[serde(default)]
    branch: String,
    /// Not used in our system.
    #[serde(default)]
    headnumber: String,
    /// Not used in our system, but the devices set them as an incremental `triggertime` so that when looking into the logs we see easy incremental numbers, that we can understand.
    ///
    /// Also an event identifier.
    ///
    /// Retried events have the same incremental_id as the original event.
    ///
    /// If running the `mqtt` simulation, this field would be `None`, when it arrives in the server. The `python` proxy that receives this `SimpleCall`, does not have this field, so its lost.
    #[serde(default)]
    incremental_id: Option<u64>,
}

impl SimpleCall {
    pub fn new_now(phone: String) -> Self {
        Self {
            phone,
            triggertime: Utc::now(),
            branch: String::from("Branch"),
            headnumber: String::from("Headnumber"),
            incremental_id: None,
        }
    }

    pub fn with_incremental_id(mut self, id: u64) -> Self {
        self.incremental_id = Some(id);
        self
    }
}

/// This is the event our system processes.
///
/// It wraps the original event sent from the device, and adds some metadata to it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// The device_id helps us ensure in order delivery of events from the same device.
    pub device_id: String,
    /// The event_id helps us prevent duplicated events.
    ///
    /// It should also impl [`Ord`] and [`Eq`] to ensure that events can be ordered and compared.
    pub event_id: u64,
    /// The arrived_at is the time when the event arrives at our server.
    ///
    /// It is used to measure the latency of the event processing.
    ///
    /// Events are created without this field, and it is filled when the event arrives at our server.
    pub arrived_at: Option<DateTime<Utc>>,
    /// The finished_at is the time when the event processing is finished.
    ///
    /// It is used to measure the latency of the event processing.
    ///
    /// Events are created without this field, and it is filled when the event processing is finished.
    pub finished_at: Option<DateTime<Utc>>,
    /// This is the original event [`SimpleCall`].
    ///
    /// We intentionally erase it, since our system does not care about the content of the event.
    pub original: serde_json::Value,
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.device_id == other.device_id && self.event_id == other.event_id
    }
}

impl Eq for Event {}

impl Hash for Event {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.device_id.hash(state);
        self.event_id.hash(state);
    }
}

impl From<SimpleCall> for Event {
    fn from(call: SimpleCall) -> Self {
        Self {
            device_id: call.phone.clone(),
            event_id: call.triggertime.timestamp_millis() as u64,
            arrived_at: None,
            finished_at: None,
            original: serde_json::to_value(call).expect("Failed to serialize SimpleCall"),
        }
    }
}

impl Event {
    pub fn with_arrived_at_now(mut self) -> Self {
        self.arrived_at = Some(Utc::now());
        self
    }

    pub fn with_finished_at_now(mut self) -> Self {
        self.finished_at = Some(Utc::now());
        self
    }

    fn process_time(&self) -> Option<Duration> {
        match (self.arrived_at, self.finished_at) {
            (Some(arrived), Some(finished)) => {
                let duration = finished.signed_duration_since(arrived);

                let micros = duration.num_microseconds()?;

                if micros < 0 {
                    return None;
                }

                Some(Duration::from_micros(micros as u64))
            }
            _ => None,
        }
    }

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
        events: impl Iterator<Item = &'a Event> + 'a,
    ) -> Result<(), OrderError> {
        let mut last = HashMap::<String, u64>::new();
        let mut violations = Vec::new();

        for event in events {
            if let Some(&prev_max) = last.get(&event.device_id)
                && event.event_id < prev_max
            {
                violations.push(OrderViolation::new(
                    event.device_id.clone(),
                    event.event_id,
                    prev_max,
                ));
            }

            last.insert(event.device_id.clone(), event.event_id);

            // last.entry(event.device_id.clone())
            //     .and_modify(|m| *m = (*m).max(event.event_id))
            //     .or_insert(event.event_id);
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(OrderError::new(violations))
        }
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

    pub fn calculate_median_latency_ms<'a>(
        events: impl Iterator<Item = &'a Event>,
    ) -> Result<f64, anyhow::Error> {
        let mut latencies: Vec<f64> = events
            .filter_map(|event| {
                event.arrived_at.map(|arrived| {
                    let arrived_ms = arrived.timestamp_millis() as f64;
                    let trigger_ms = event.event_id as f64;

                    arrived_ms - trigger_ms
                })
            })
            .collect();

        if latencies.is_empty() {
            return Err(anyhow::anyhow!(
                "No events with arrival times found to calculate latency"
            ));
        }

        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let len = latencies.len();
        let mid = len / 2;

        let median = if len.is_multiple_of(2) {
            (latencies[mid - 1] + latencies[mid]) / 2.0
        } else {
            latencies[mid]
        };

        Ok(median)
    }

    pub fn calculate_average_latency_ms<'a>(
        events: impl Iterator<Item = &'a Event>,
    ) -> Result<f64, anyhow::Error> {
        let latencies: Vec<f64> = events
            .filter_map(|event| {
                event.arrived_at.map(|arrived| {
                    let arrived_ms = arrived.timestamp_millis() as f64;
                    let trigger_ms = event.event_id as f64;

                    arrived_ms - trigger_ms
                })
            })
            .collect();

        if latencies.is_empty() {
            return Err(anyhow::anyhow!(
                "No events with arrival times found to calculate latency"
            ));
        }

        let sum: f64 = latencies.iter().sum();
        let average = sum / latencies.len() as f64;

        Ok(average)
    }

    pub fn calculate_p99_latency_ms<'a>(
        events: impl Iterator<Item = &'a Event>,
    ) -> Result<f64, anyhow::Error> {
        let mut latencies: Vec<f64> = events
            .filter_map(|event| {
                event.arrived_at.map(|arrived| {
                    let arrived_ms = arrived.timestamp_millis() as f64;
                    let trigger_ms = event.event_id as f64;

                    arrived_ms - trigger_ms
                })
            })
            .collect();

        if latencies.is_empty() {
            return Err(anyhow::anyhow!(
                "No events with arrival times found to calculate latency"
            ));
        }

        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let len = latencies.len();
        // Nearest-rank method: index of the 99th percentile.
        let rank = ((0.99 * len as f64).ceil() as usize).clamp(1, len);
        let index = rank - 1;

        Ok(latencies[index])
    }

    pub fn calculate_process_median_latency_ms<'a>(
        events: impl Iterator<Item = &'a Event>,
    ) -> Result<f64, anyhow::Error> {
        let mut latencies: Vec<f64> = events
            .filter_map(|event| event.process_time().map(|d| d.as_secs_f64() * 1000.0))
            .collect();

        if latencies.is_empty() {
            return Err(anyhow::anyhow!(
                "No events with both arrival and finish times found to calculate process latency"
            ));
        }

        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let len = latencies.len();
        let mid = len / 2;

        let median = if len.is_multiple_of(2) {
            (latencies[mid - 1] + latencies[mid]) / 2.0
        } else {
            latencies[mid]
        };

        Ok(median)
    }

    pub fn calculate_process_average_latency_ms<'a>(
        events: impl Iterator<Item = &'a Event>,
    ) -> Result<f64, anyhow::Error> {
        let latencies: Vec<f64> = events
            .filter_map(|event| event.process_time().map(|d| d.as_secs_f64() * 1000.0))
            .collect();

        if latencies.is_empty() {
            return Err(anyhow::anyhow!(
                "No events with both arrival and finish times found to calculate process latency"
            ));
        }

        let sum: f64 = latencies.iter().sum();
        let average = sum / latencies.len() as f64;

        Ok(average)
    }

    pub fn calculate_process_p99_latency_ms<'a>(
        events: impl Iterator<Item = &'a Event>,
    ) -> Result<f64, anyhow::Error> {
        let mut latencies: Vec<f64> = events
            .filter_map(|event| event.process_time().map(|d| d.as_secs_f64() * 1000.0))
            .collect();

        if latencies.is_empty() {
            return Err(anyhow::anyhow!(
                "No events with both arrival and finish times found to calculate process latency"
            ));
        }

        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let len = latencies.len();
        let rank = ((0.99 * len as f64).ceil() as usize).clamp(1, len);
        let index = rank - 1;

        Ok(latencies[index])
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderViolation {
    pub device_id: String,
    pub first_event_id: u64,
    pub second_event_id: u64,
}

impl OrderViolation {
    fn new(device_id: String, first: u64, second: u64) -> Self {
        Self {
            device_id,
            first_event_id: first,
            second_event_id: second,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Found {} ordering errors: {violations:?}", violations.len())]
pub struct OrderError {
    pub violations: Vec<OrderViolation>,
}

impl OrderError {
    fn new(violations: Vec<OrderViolation>) -> Self {
        Self { violations }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Missing {} events: {missing:?}", missing.len())]
pub struct CompletenessError {
    pub missing: Vec<Event>,
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
/// - A JSON object with the correct fields directly.
/// - A JSON object with a `body` field containing either:
///   - A JSON string representing the correct object.
///   - A Python [`str`]-formatted string (`"Phone: X, Branch: Y, Headnumber: Z, TriggerTime: T"`).
/// - A string containing a JSON object, handled recursively via the above cases.
/// - A bare Python [`str`]-formatted string directly.
const _: () = {
    use serde::{Deserialize, Deserializer, de};

    #[derive(Debug, Deserialize)]
    struct SimpleCallHelper {
        phone: String,
        triggertime: DateTime<Utc>,

        #[serde(default)]
        branch: String,

        #[serde(default)]
        headnumber: String,

        #[serde(default)]
        incremental_id: Option<u64>,
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
                incremental_id: data.incremental_id,
            });
        }

        // Case 2: wrapper object with body
        if let Ok(wrapper) = serde_json::from_value::<BodyWrapper>(value.clone()) {
            let body = &wrapper.body;

            // Case 2a: body is a JSON string
            if let Ok(data) = serde_json::from_str::<SimpleCallHelper>(body) {
                return Ok(SimpleCall {
                    phone: data.phone,
                    triggertime: data.triggertime,
                    branch: data.branch,
                    headnumber: data.headnumber,
                    incremental_id: data.incremental_id,
                });
            }

            // Case 2b: body is the Python __str__ format
            return parse_simplecall_python_str(body);
        }

        // Case 3: top-level string containing wrapper object or Python __str__ format
        if let serde_json::Value::String(s) = value {
            // Case 3a: try parsing as a JSON value first (recursive)
            if let Ok(nested) = serde_json::from_str::<serde_json::Value>(&s) {
                return parse_simplecall_value(nested);
            }

            // Case 3b: try parsing as Python __str__ format directly
            return parse_simplecall_python_str(&s);
        }

        Err("invalid SimpleCall format".into())
    }

    /// Parses the Python `__str__` format:
    /// `Phone: <phone>, Branch: <branch>, Headnumber: <headnumber>, TriggerTime: <triggertime>`
    fn parse_simplecall_python_str(s: &str) -> Result<SimpleCall, String> {
        let mut phone = None;
        let mut branch = None;
        let mut headnumber = None;
        let mut triggertime_str = None;

        // The string looks like "Phone: X, Branch: Y, Headnumber: Z, TriggerTime: T"
        // TriggerTime must be captured last since it may contain commas (though RFC3339 won't).
        // We split on ", " but only at known key boundaries to be safe.
        let keys = ["Phone: ", "Branch: ", "Headnumber: ", "TriggerTime: "];

        // Find each key and extract the value up to the next key (or end of string).
        for (i, key) in keys.iter().enumerate() {
            let start = s
                .find(key)
                .ok_or_else(|| format!("missing field '{key}' in Python str format"))?;
            let value_start = start + key.len();

            // Value ends at the start of the next known key (minus the ", "), or end of string.
            let value_end = keys[i + 1..]
                .iter()
                .filter_map(|next_key| {
                    // The separator before each key is ", "
                    s[value_start..]
                        .find(&format!(", {next_key}"))
                        .map(|pos| value_start + pos)
                })
                .min()
                .unwrap_or(s.len());

            let value = s[value_start..value_end].trim().to_string();

            match *key {
                "Phone: " => phone = Some(value),
                "Branch: " => branch = Some(value),
                "Headnumber: " => headnumber = Some(value),
                "TriggerTime: " => triggertime_str = Some(value),
                _ => {}
            }
        }

        let phone = phone.ok_or("missing phone")?;
        let branch = branch.ok_or("missing branch")?;
        let headnumber = headnumber.ok_or("missing headnumber")?;
        let triggertime_str = triggertime_str.ok_or("missing triggertime")?;

        // Python datetime str format: "2026-03-26 10:30:00+00:00" (space instead of T, offset instead of Z)
        // DateTime<Utc> can parse RFC3339, so we normalise it first.
        let normalised = triggertime_str.replacen(' ', "T", 1);
        let triggertime = DateTime::parse_from_rfc3339(&normalised)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| format!("invalid triggertime '{triggertime_str}': {e}"))?;

        Ok(SimpleCall {
            phone,
            triggertime,
            branch,
            headnumber,
            incremental_id: None,
        })
    }
};

#[derive(Debug, Serialize)]
pub struct Failure {
    failed_at: DateTime<Utc>,
    event: Event,
}

impl Failure {
    pub fn new_now(event: Event) -> Self {
        Self {
            failed_at: Utc::now(),
            event,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TcpConnectionEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    dropped_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    accepted_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refused_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed_at: Option<DateTime<Utc>>,
}

impl TcpConnectionEvent {
    pub fn new_dropped_now() -> Self {
        Self {
            dropped_at: Some(Utc::now()),
            accepted_at: None,
            refused_at: None,
            completed_at: None,
        }
    }

    pub fn new_accepted_now() -> Self {
        Self {
            dropped_at: None,
            accepted_at: Some(Utc::now()),
            refused_at: None,
            completed_at: None,
        }
    }

    pub fn new_refused_now() -> Self {
        Self {
            dropped_at: None,
            accepted_at: None,
            refused_at: Some(Utc::now()),
            completed_at: None,
        }
    }

    pub fn new_completed_now() -> Self {
        Self {
            dropped_at: None,
            accepted_at: None,
            refused_at: None,
            completed_at: Some(Utc::now()),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Rates {
    // These fields represent the latency of the events, i.e. the time between when the event was triggered and when it arrived at the server.
    pub median_latency_ms: f64,
    pub average_latency_ms: f64,
    pub p99_latency_ms: f64,

    // These fields represent the processing latency of the events, i.e. the time between when the event arrived at the server and when it finished processing.
    pub process_median_latency_ms: f64,
    pub process_average_latency_ms: f64,
    pub process_p99_latency_ms: f64,

    pub duplication_rate: f64,
    pub out_of_order_rate: f64,
    pub missing_rate: f64,
    pub total_events: usize,
    pub unique_events: usize,
    pub duplicate_events: usize,
    pub out_of_order_events: usize,
    pub missing_events: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

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
    fn deserialize_python_str_simplecall() {
        // Simulates what Python's __str__ produces:
        // f"Phone: {phone}, Branch: {branch}, Headnumber: {headnumber}, TriggerTime: {triggertime}"
        // Python datetime UTC str looks like "2026-03-26 10:30:00+00:00"
        let json = r#""Phone: 123456, Branch: A, Headnumber: 999, TriggerTime: 2026-03-26 10:30:00+00:00""#;

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
    fn deserialize_python_str_wrapped_in_body() {
        // The Python str format wrapped in a body field (e.g. sent as an event body)
        let inner =
            "Phone: 123456, Branch: A, Headnumber: 999, TriggerTime: 2026-03-26 10:30:00+00:00";
        let json = format!(r#"{{"body": "{inner}"}}"#);

        let call: SimpleCall = serde_json::from_str(&json).unwrap();

        assert_eq!(call.phone, "123456");
        assert_eq!(call.branch, "A");
        assert_eq!(call.headnumber, "999");
        assert_eq!(
            call.triggertime,
            Utc.with_ymd_and_hms(2026, 3, 26, 10, 30, 0).unwrap()
        );
    }

    #[test]
    fn validate_order() {
        // #1 (08:00), #2 (09:00), #3 (09:01), #4 (08:01), #5 (08:05)
        let device = "device-1".to_string();

        let make_event = |hour: u32, minute: u32| Event {
            device_id: device.clone(),
            event_id: Utc
                .with_ymd_and_hms(2026, 3, 26, hour, minute, 0)
                .unwrap()
                .timestamp_millis() as u64,
            arrived_at: None,
            finished_at: None,
            original: serde_json::Value::Null,
        };

        // #1 (08:00), #2 (09:00), #3 (09:01), #4 (08:01), #5 (08:05)
        let events = [
            make_event(8, 0),
            make_event(9, 0),
            make_event(9, 1),
            make_event(8, 1),
            make_event(8, 5),
        ];

        // Map event_id -> "#N (HH:MM)" so violations print in human-readable form.
        let labels: HashMap<u64, String> = events
            .iter()
            .enumerate()
            .map(|(i, e)| {
                let dt = Utc.timestamp_millis_opt(e.event_id as i64).unwrap();
                (e.event_id, format!("#{} ({})", i + 1, dt.format("%H:%M")))
            })
            .collect();

        let label = |id: u64| labels.get(&id).cloned().unwrap_or_else(|| id.to_string());

        let err = Event::validate_order(events.iter()).unwrap_err();

        let readable: Vec<String> = err
            .violations
            .iter()
            .map(|v| {
                format!(
                    "device={} {} came after {}",
                    v.device_id,
                    label(v.first_event_id),
                    label(v.second_event_id)
                )
            })
            .collect();

        println!("{}, {:#?}", err.violations.len(), readable);
    }
}
