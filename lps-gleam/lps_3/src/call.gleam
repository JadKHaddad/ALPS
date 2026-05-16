import gleam/dynamic/decode
import gleam/json
import gleam/list
import gleam/string

/// See the rust version docs to understand why we deserialize the call in multiple ways
pub type SimpleCall {
  SimpleCall(
    phone: String,
    triggertime: String,
    branch: String,
    headnumber: String,
  )
}

pub fn to_json(call: SimpleCall) -> json.Json {
  let SimpleCall(phone, triggertime, branch, headnumber) = call

  json.object([
    #("phone", json.string(phone)),
    #("triggertime", json.string(triggertime)),
    #("branch", json.string(branch)),
    #("headnumber", json.string(headnumber)),
  ])
}

pub fn decoder() -> decode.Decoder(SimpleCall) {
  decode.one_of(direct_decoder(), [
    body_wrapper_decoder(),
    string_wrapper_decoder(),
  ])
}

fn direct_decoder() -> decode.Decoder(SimpleCall) {
  use phone <- decode.field("phone", decode.string)
  use triggertime <- decode.field("triggertime", decode.string)
  use branch <- decode.optional_field("branch", "", decode.string)
  use headnumber <- decode.optional_field("headnumber", "", decode.string)
  decode.success(SimpleCall(phone, triggertime, branch, headnumber))
}

fn body_wrapper_decoder() -> decode.Decoder(SimpleCall) {
  use body <- decode.field("body", decode.string)
  // Case 2a: body is JSON
  // Case 2b: body is Python __str__ format
  case json.parse(body, decoder()) {
    Ok(call) -> decode.success(call)
    Error(_) ->
      case parse_python_str(body) {
        Ok(call) -> decode.success(call)
        Error(msg) -> decode.failure(SimpleCall("", "", "", ""), msg)
      }
  }
}

fn string_wrapper_decoder() -> decode.Decoder(SimpleCall) {
  use s <- decode.then(decode.string)
  // Case 3a: top-level string containing JSON (recursive via decoder())
  // Case 3b: top-level Python __str__ format
  case json.parse(s, decoder()) {
    Ok(call) -> decode.success(call)
    Error(_) ->
      case parse_python_str(s) {
        Ok(call) -> decode.success(call)
        Error(msg) -> decode.failure(SimpleCall("", "", "", ""), msg)
      }
  }
}

/// Parses the Python `__str__` format:
/// `Phone: <phone>, Branch: <branch>, Headnumber: <headnumber>, TriggerTime: <triggertime>`
///
/// Each value is extracted by finding the key prefix and reading up to the
/// next known key boundary (or end of string), matching the Rust implementation.
fn parse_python_str(s: String) -> Result(SimpleCall, String) {
  let keys = ["Phone: ", "Branch: ", "Headnumber: ", "TriggerTime: "]

  use phone <- result_then(extract_field(s, "Phone: ", keys))
  use branch <- result_then(extract_field(s, "Branch: ", keys))
  use headnumber <- result_then(extract_field(s, "Headnumber: ", keys))
  use triggertime_raw <- result_then(extract_field(s, "TriggerTime: ", keys))

  // Normalise Python datetime "2026-03-26 10:30:00+00:00" → "2026-03-26T10:30:00+00:00"
  // We keep triggertime as a string (no chrono in Gleam here), but we normalise
  // the space→T so downstream RFC3339 parsing works the same way the Rust code does.
  let triggertime = normalise_datetime(triggertime_raw)

  Ok(SimpleCall(phone, triggertime, branch, headnumber))
}

/// Finds `key` in `s`, then reads the value up to the earliest occurrence of
/// any subsequent key preceded by ", " — or to the end of the string.
fn extract_field(
  s: String,
  key: String,
  all_keys: List(String),
) -> Result(String, String) {
  case string.split_once(s, key) {
    Error(_) -> Error("missing field '" <> key <> "' in Python str format")
    Ok(#(_, after_key)) -> {
      // Collect the position of every ", <next_key>" that appears after our value start.
      // We drop keys up-to-and-including the current one so we only look ahead.
      let following_keys = drop_up_to(all_keys, key)

      let end_pos =
        list.filter_map(following_keys, fn(next_key) {
          let separator = ", " <> next_key
          case string.split_once(after_key, separator) {
            Ok(#(before, _)) -> Ok(string.length(before))
            Error(_) -> Error(Nil)
          }
        })
        |> list.fold(string.length(after_key), int_min)

      Ok(string.slice(after_key, 0, end_pos) |> string.trim)
    }
  }
}

/// Drops all elements up to and including the first occurrence of `target`.
fn drop_up_to(keys: List(String), target: String) -> List(String) {
  case keys {
    [] -> []
    [first, ..rest] ->
      case first == target {
        True -> rest
        False -> drop_up_to(rest, target)
      }
  }
}

fn int_min(a: Int, b: Int) -> Int {
  case a < b {
    True -> a
    False -> b
  }
}

/// Replace the first space with "T" to turn a Python datetime string into
/// an RFC3339-compatible one: "2026-03-26 10:30:00+00:00" → "2026-03-26T10:30:00+00:00"
fn normalise_datetime(s: String) -> String {
  case string.split_once(s, " ") {
    Ok(#(date, time)) -> date <> "T" <> time
    Error(_) -> s
  }
}

/// Monadic bind for Result, mirroring `use x <- result_then(expr)` ergonomics.
fn result_then(result: Result(a, e), f: fn(a) -> Result(b, e)) -> Result(b, e) {
  case result {
    Ok(v) -> f(v)
    Error(e) -> Error(e)
  }
}
