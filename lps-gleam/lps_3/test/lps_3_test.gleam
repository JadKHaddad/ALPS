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
