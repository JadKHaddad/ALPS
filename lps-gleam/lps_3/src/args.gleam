import argv
import clip
import clip/help
import clip/opt

pub type Args {
  Args(config_file: String)
}

fn config_file_opt() {
  opt.new("config-file")
  |> opt.help("Config file: path to the configuration file")
  |> opt.default("lps3-config.yaml")
}

fn command() {
  clip.command({
    use config_file <- clip.parameter
    Args(config_file)
  })
  |> clip.opt(config_file_opt())
}

pub fn run() {
  command()
  |> clip.help(help.simple("lps3", "Legacy Proxy Server 3"))
  |> clip.run(argv.load().arguments)
}
