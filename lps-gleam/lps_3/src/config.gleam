import glaml
import gleam/list
import simplifile

pub type Config {
  Config(
    port: Int,
    server_push_url: String,
    nodes: List(String),
    bootstrap: Bool,
  )
}

pub fn default_config() -> Config {
  Config(
    port: 4000,
    server_push_url: "http://localhost:8080/work",
    nodes: [],
    bootstrap: False,
  )
}

fn decode_config(doc: glaml.Node) -> Result(Config, String) {
  let port = case glaml.select_sugar(doc, "port") {
    Ok(glaml.NodeInt(p)) -> p
    _ -> default_config().port
  }

  let server_push_url = case glaml.select_sugar(doc, "server_push_url") {
    Ok(glaml.NodeStr(url)) -> url
    _ -> default_config().server_push_url
  }

  let nodes = case glaml.select_sugar(doc, "nodes") {
    Ok(glaml.NodeSeq(items)) ->
      list.filter_map(items, fn(item) {
        case item {
          glaml.NodeStr(s) -> Ok(s)
          _ -> Error(Nil)
        }
      })
    _ -> default_config().nodes
  }

  let bootstrap = case glaml.select_sugar(doc, "bootstrap") {
    Ok(glaml.NodeBool(b)) -> b
    _ -> default_config().bootstrap
  }

  Ok(Config(
    port: port,
    server_push_url: server_push_url,
    nodes: nodes,
    bootstrap: bootstrap,
  ))
}

pub fn load_config(path: String) -> Result(Config, String) {
  case simplifile.read(path) {
    Error(_) -> Ok(default_config())

    Ok(contents) ->
      case glaml.parse_string(contents) {
        Ok([]) -> Ok(default_config())
        Ok([doc, ..]) -> decode_config(glaml.document_root(doc))
        Error(_) -> Error("Failed to parse YAML")
      }
  }
}
