import glaml
import simplifile

pub type Config {
  Config(server_push_url: String)
}

pub fn default_config() -> Config {
  Config(server_push_url: "http://localhost:8080/work")
}

fn decode_config(doc: glaml.Document) -> Result(Config, String) {
  case glaml.select_sugar(glaml.document_root(doc), "server_push_url") {
    Ok(glaml.NodeStr(url)) -> Ok(Config(server_push_url: url))

    Ok(_) -> Error("server_push_url must be a string")

    Error(_) -> Ok(default_config())
  }
}

pub fn load_config(path: String) -> Result(Config, String) {
  case simplifile.read(path) {
    Error(_) -> Ok(default_config())

    Ok(contents) -> {
      case glaml.parse_string(contents) {
        Error(_) -> Error("Failed to parse YAML")

        Ok([doc, ..]) -> decode_config(doc)

        Ok([]) -> Ok(default_config())
      }
    }
  }
}
