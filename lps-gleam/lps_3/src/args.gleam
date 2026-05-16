import argv
import clip
import clip/flag
import clip/help
import clip/opt

pub type Args {
  Args(config_file: String, bootstrap: Bool)
}

fn config_file_opt() {
  opt.new("config-file")
  |> opt.help("Config file: path to the configuration file")
  |> opt.default("lps3-config.yaml")
}

fn bootstrap_flag() {
  flag.new("bootstrap")
  |> flag.help("Bootstrap: bootstrap mnesia on startup")
}

fn command() {
  clip.command({
    use config_file <- clip.parameter
    use bootstrap <- clip.parameter
    Args(config_file, bootstrap)
  })
  |> clip.opt(config_file_opt())
  |> clip.flag(bootstrap_flag())
}

pub fn run() {
  command()
  |> clip.help(help.simple("lps3", "Legacy Proxy Server 3"))
  |> clip.run(argv.load().arguments)
}
