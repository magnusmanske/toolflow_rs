use anyhow::Result;
use lazy_static::lazy_static;
use app::App;
use clap::{arg, Command};

use crate::renderer::{RendererWikitext, Renderer};

pub mod app;
pub mod data_file;
pub mod wiki_page;
pub mod mapping;
pub mod renderer;
pub mod adapter;
pub mod join;
pub mod filter;
pub mod generator;
pub mod data_cell;
pub mod data_header;
pub mod workflow;
pub mod workflow_node;
pub mod workflow_run;

lazy_static! {
    static ref APP: App = App::new();
}

fn cli() -> Command {
    Command::new("toolflow")
        .about("ToolFlow server and command line utility")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .subcommand(
            Command::new("server")
                .about("Runs the ToolFlow server")
                // .arg(arg!(<REMOTE> "The remote to clone"))
                // .arg_required_else_help(true),
        )
        .subcommand(
            Command::new("render")
                .about("Runs a renderer")
                .arg(arg!(mode: [MODE]))
                .arg(arg!(uuid: [UUID]))
                // .arg(arg!(<MISC> "Misc parameters, depnding on renderer type"))
                .arg_required_else_help(true),
        )
}

#[tokio::main]
async fn main() -> Result<()> {
    let matches = cli().get_matches();

    match matches.subcommand() {
        Some(("server", _sub_matches)) => {
            APP.server().await
        },
        Some(("render", sub_matches)) => {
            let mode = sub_matches.get_one::<String>("mode").map(|s| s.as_str()).expect("mode not set");
            let uuid = sub_matches.get_one::<String>("uuid").map(|s| s.as_str()).expect("uuid not set");
            // let _misc = sub_matches.get_one::<String>("misc").map(|s| s.as_str());
            match mode {
                "wiki" => {
                    let wikitext = RendererWikitext::default().render_from_uuid(uuid).expect(&format!("No data file for uuid {uuid}"));
                    println!("{wikitext}");
                }
                other => panic!("Render type '{other}' is not supported"),
            }
            Ok(())
        }
        _ => unreachable!(), // If all subcommands are defined above, anything else is unreachable!()
    }
}

/*
ssh magnus@tools-login.wmflabs.org -L 3306:tools-db:3306 -N &

toolforge jobs run --image tf-php74 --mem 3Gi --cpu 1 --continuous --command '/data/project/toolflow/toolflow_rs/run.sh' toolflow-server
rm -f ~/toolflow-server.* ; toolforge jobs restart toolflow-server
clear ; toolforge jobs list ; tail ~/toolflow-server.*

*/

// rsync -azv /Users/mm6/rust/toolflow/tmp/* magnus@tools-login.wmflabs.org:/data/project/toolflow/data
