use std::time::SystemTime;

use lazy_static::lazy_static;
use app::App;
use workflow::Workflow;
use workflow_run::WorkflowNodeStatusValue;

pub mod app;
pub mod data_file;
pub mod wiki_page;
pub mod mapping;
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

#[tokio::main]
async fn main() {
    let mut conn = match APP.get_db_connection().await {
        Ok(conn) => conn,
        Err(e) => panic!("{e}"),
    };

    let _ = APP.clear_old_files().await;
    let mut last_clear_time = SystemTime::now();


    loop {
        match last_clear_time.elapsed() {
            Ok(elapsed) => {
                if elapsed.as_secs()>5*60 { // Every 5 minutes
                    let _ = APP.clear_old_files().await;
                    last_clear_time = SystemTime::now();
                }
            }
            Err(_) => {},
        }

        match APP.find_next_waiting_run(&mut conn).await {
            Some((run_id,workflow_id)) => {
                let mut workflow = match Workflow::from_id(workflow_id).await {
                    Ok(workflow) => workflow,
                    Err(e) => {
                        eprintln!("Cannot get workflow {workflow_id}: {e}");
                        continue;
                    }
                };
                workflow.run.set_id(run_id);
                if let Err(e) = workflow.run.update_status(WorkflowNodeStatusValue::RUNNING, &mut conn).await {
                    eprintln!("Cannot update initial status: {e}");
                    continue;
                }
                println!("Starting workflow {workflow_id} run {run_id}");
                tokio::spawn(async move {
                    println!("Started workflow {workflow_id} run {run_id}");
                    let result = workflow.run().await;
                    println!("Finished workflow {workflow_id} run {run_id}: {result:?}");
                });

            }
            None => APP.hold_on(),
        }
    }
}

/*
ssh magnus@tools-login.wmflabs.org -L 3306:tools-db:3306 -N &

toolforge jobs run --image tf-php74 --mem 3Gi --cpu 1 --continuous --command '/data/project/toolflow/toolflow_rs/run.sh' toolflow-server
rm -f ~/toolflow-server.* ; toolforge jobs restart toolflow-server
clear ; toolforge jobs list ; tail ~/toolflow-server.*

*/

// rsync -azv /Users/mm6/rust/toolflow/tmp/* magnus@tools-login.wmflabs.org:/data/project/toolflow/data
