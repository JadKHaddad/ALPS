import call
import gleam/json
import gleeunit
import gleeunit/should

pub fn main() {
  gleeunit.main()
}

pub fn deserialize_direct_simplecall_test() {
  let json_str =
    "{\"phone\":\"123456\",\"branch\":\"A\",\"headnumber\":\"999\",\"triggertime\":\"2026-03-26T10:30:00Z\"}"

  let result = json.parse(json_str, call.decoder())

  result
  |> should.be_ok
  |> fn(call) {
    call.phone |> should.equal("123456")
    call.branch |> should.equal("A")
    call.headnumber |> should.equal("999")
    call.triggertime |> should.equal("2026-03-26T10:30:00Z")
  }
}

pub fn deserialize_wrapped_body_simplecall_test() {
  let json_str =
    "{\"body\":\"{\\\"phone\\\":\\\"123456\\\",\\\"branch\\\":\\\"A\\\",\\\"headnumber\\\":\\\"999\\\",\\\"triggertime\\\":\\\"2026-03-26T10:30:00Z\\\"}\"}"

  let result = json.parse(json_str, call.decoder())

  result
  |> should.be_ok
  |> fn(call) {
    call.phone |> should.equal("123456")
    call.branch |> should.equal("A")
    call.headnumber |> should.equal("999")
    call.triggertime |> should.equal("2026-03-26T10:30:00Z")
  }
}

pub fn deserialize_string_wrapper_simplecall_test() {
  let json_str =
    "\"{\\\"id\\\":\\\"9\\\",\\\"body\\\":\\\"{\\\\\\\"phone\\\\\\\":\\\\\\\"9\\\\\\\",\\\\\\\"branch\\\\\\\":\\\\\\\"Branch\\\\\\\",\\\\\\\"headnumber\\\\\\\":\\\\\\\"Headnumber\\\\\\\",\\\\\\\"triggertime\\\\\\\":\\\\\\\"2026-03-26T09:49:37.044594Z\\\\\\\"}\\\",\\\"request_id\\\":\\\"device-9-event-0-try-1\\\"}\""

  let result = json.parse(json_str, call.decoder())

  result
  |> should.be_ok
  |> fn(call) {
    call.phone |> should.equal("9")
    call.branch |> should.equal("Branch")
    call.headnumber |> should.equal("Headnumber")
    call.triggertime |> should.equal("2026-03-26T09:49:37.044594Z")
  }
}

pub fn deserialize_python_str_direct_test() {
  // Top-level bare Python __str__ format
  let json_str =
    "\"Phone: 123456, Branch: A, Headnumber: 999, TriggerTime: 2026-03-26 10:30:00+00:00\""

  let result = json.parse(json_str, call.decoder())

  result
  |> should.be_ok
  |> fn(call) {
    call.phone |> should.equal("123456")
    call.branch |> should.equal("A")
    call.headnumber |> should.equal("999")
    // Space should be normalised to T
    call.triggertime |> should.equal("2026-03-26T10:30:00+00:00")
  }
}

pub fn deserialize_python_str_in_body_test() {
  // Python __str__ format wrapped in a body field
  let json_str =
    "{\"body\":\"Phone: 123456, Branch: A, Headnumber: 999, TriggerTime: 2026-03-26 10:30:00+00:00\"}"

  let result = json.parse(json_str, call.decoder())

  result
  |> should.be_ok
  |> fn(call) {
    call.phone |> should.equal("123456")
    call.branch |> should.equal("A")
    call.headnumber |> should.equal("999")
    call.triggertime |> should.equal("2026-03-26T10:30:00+00:00")
  }
}

pub fn deserialize_python_str_already_has_t_separator_test() {
  // If the datetime already has a T (RFC3339), normalise_datetime should be a no-op
  let json_str =
    "\"Phone: 123456, Branch: A, Headnumber: 999, TriggerTime: 2026-03-26T10:30:00Z\""

  let result = json.parse(json_str, call.decoder())

  result
  |> should.be_ok
  |> fn(call) { call.triggertime |> should.equal("2026-03-26T10:30:00Z") }
}

pub fn deserialize_python_str_empty_branch_and_headnumber_test() {
  // Fields present but empty — parser should still find the keys and return ""
  let json_str =
    "\"Phone: 123456, Branch: , Headnumber: , TriggerTime: 2026-03-26 10:30:00+00:00\""

  let result = json.parse(json_str, call.decoder())

  result
  |> should.be_ok
  |> fn(call) {
    call.phone |> should.equal("123456")
    call.branch |> should.equal("")
    call.headnumber |> should.equal("")
    call.triggertime |> should.equal("2026-03-26T10:30:00+00:00")
  }
}

pub fn deserialize_python_str_triggertime_with_offset_test() {
  // Timezone offset contains a colon — must not be split on it
  let json_str =
    "\"Phone: 123456, Branch: A, Headnumber: 999, TriggerTime: 2026-03-26 10:30:00+05:30\""

  let result = json.parse(json_str, call.decoder())

  result
  |> should.be_ok
  |> fn(call) { call.triggertime |> should.equal("2026-03-26T10:30:00+05:30") }
}

pub fn deserialize_python_str_missing_field_is_error_test() {
  // Completely missing a required key should fail, not panic
  let json_str =
    "\"Phone: 123456, Branch: A, TriggerTime: 2026-03-26 10:30:00+00:00\""

  json.parse(json_str, call.decoder())
  |> should.be_error
}

pub fn deserialize_invalid_string_is_error_test() {
  // A top-level string that is neither JSON nor Python str format
  let json_str = "\"this is just a random string\""

  json.parse(json_str, call.decoder())
  |> should.be_error
}

pub fn deserialize_body_with_invalid_content_is_error_test() {
  // body field present but content is neither JSON nor Python str format
  let json_str = "{\"body\":\"not a valid format at all\"}"

  json.parse(json_str, call.decoder())
  |> should.be_error
}
