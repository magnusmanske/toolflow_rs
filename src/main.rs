use lazy_static::lazy_static;
use app::App;
use workflow::Workflow;
use workflow_run::WorkflowNodeStatusValue;

pub mod app;
pub mod data_file;
pub mod wiki_page;
pub mod mapping;
pub mod adapter;
pub mod data_header;
pub mod workflow;
pub mod workflow_node;
pub mod workflow_run;

lazy_static! {
    static ref APP: App = App::new();
}

#[tokio::main]
async fn main() {
    let _ = APP.clear_old_files().await;

    let mut conn = match APP.get_db_connection().await {
        Ok(conn) => conn,
        Err(e) => panic!("{e}"),
    };

    loop {
        match APP.find_next_waiting_run(&mut conn).await {
            Some((run_id,workflow_id)) => {
                let mut workflow = Workflow::from_id(workflow_id).await.unwrap();
                workflow.run.set_id(run_id);
                workflow.run.update_status(WorkflowNodeStatusValue::RUNNING, &mut conn).await.unwrap();
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

*/

// rsync -azv /Users/mm6/rust/toolflow/tmp/* magnus@tools-login.wmflabs.org:/data/project/toolflow/data

/*
use serde_json::json;
use mapping::HeaderMapping;
use workflow::{Workflow, WorkflowNode, WorkflowNodeKind, WorkflowEdge};
use crate::wiki_page::WikiPage;


    let mut nodes = vec![];

    // Quarry
    let parameters = vec![("query_id","76272")]
        .iter().map(|(k,v)|(k.to_string(),v.to_string())).collect();
    let wiki_page = WikiPage::new_commons_category();
    let header_mapping = HeaderMapping::default()
        .add_wiki_page("taxon_name","commons_category",&wiki_page)
        .add_plain_text("taxon_name","taxon_name")
        .build();
    let node = WorkflowNode { 
        kind: WorkflowNodeKind::Quarry, 
        parameters, 
        header_mapping,
    };
    nodes.push(node);

    // SPARQL
    let parameters = vec![("sparql","SELECT ?q ?taxon_name { ?q wdt:P225 ?taxon_name ; wdt:P105 wd:Q7432 MINUS { ?q wdt:P18 [] } } LIMIT 50000")]
        .iter().map(|(k,v)|(k.to_string(),v.to_string())).collect();
    let mut header_mapping = HeaderMapping::default();
    header_mapping.add_wikidata_item("q","item");
    header_mapping.add_plain_text("taxon_name","taxon_name");
    let node = WorkflowNode { 
        kind: WorkflowNodeKind::Sparql, 
        parameters, 
        header_mapping
    };
    nodes.push(node);

    let parameters = vec![("mode","inner_join_on_key"),("join_key","taxon_name")]
        .iter().map(|(k,v)|(k.to_string(),v.to_string())).collect();
    let node = WorkflowNode { 
        kind: WorkflowNodeKind::Join,
        parameters, 
        header_mapping: HeaderMapping::default()
    };
    nodes.push(node);

    let mut edges = vec![];
    edges.push(WorkflowEdge { source_node: 0, target_node: 2, target_slot: 0 });
    edges.push(WorkflowEdge { source_node: 1, target_node: 2, target_slot: 1 });

    let mut workflow = Workflow::new(nodes, edges) ;
    println!("!!\n{}\n!!",json!{workflow}.to_string());
     */