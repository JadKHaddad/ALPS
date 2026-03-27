import gleam/dynamic/decode
import gleam/json

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
  case json.parse(body, decoder()) {
    Ok(call) -> decode.success(call)
    Error(_) ->
      decode.failure(
        SimpleCall("", "", "", ""),
        "valid SimpleCall JSON in body field",
      )
  }
}

fn string_wrapper_decoder() -> decode.Decoder(SimpleCall) {
  use s <- decode.then(decode.string)
  case json.parse(s, decoder()) {
    Ok(call) -> decode.success(call)
    Error(_) ->
      decode.failure(SimpleCall("", "", "", ""), "valid SimpleCall JSON string")
  }
}
