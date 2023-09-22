use anyhow::{Result, anyhow};
use mediawiki::api::Api;
use regex::Regex;
use crate::{data_file::DataFileDetails, APP};

#[derive(Default, Clone, Debug)]
pub struct Generator {
}

impl Generator {
    pub async fn wikipage(wiki: &str, page: &str, user_id: usize) -> Result<DataFileDetails> {
        let server = APP.get_webserver_for_wiki(wiki).ok_or_else(||anyhow!("Could not find web server for {wiki}"))?;
        let url = format!("https://{server}/w/api.php");
        let mut api = Api::new(&url).await?;
        APP.add_user_oauth_to_api(&mut api, user_id).await?;

        let title = mediawiki::title::Title::new_from_full(page, &api);
        let page = mediawiki::page::Page::new(title);
        let before = match page.text(&api).await {
            Ok(wikitext) => wikitext,
            Err(mediawiki::page::PageError::Missing(_)) => String::new(),
            Err(e) => return Err(anyhow!(e.to_string())),
        };

        let wiki_table = format!("TESTING");

        // TODO replace old section
        let start = "<!--TOOLFLOW GENERATOR START-->";
        let end = "<!--TOOLFLOW GENERATOR END-->";
        let re = Regex::new(&format!("{start}.*{end}")).unwrap();
        let replace_with = format!("{start}\n{wiki_table}\n{end}\n");
        let after = re.replace_all(&before,replace_with.to_owned()).to_string();
        let wikitext = if before==after { replace_with } else { format!("{before}\n{after}").trim().to_string() }; // Replace or append

        page.edit_text(&mut api, wikitext, "ToolFlow generator edit").await.map_err(|e|anyhow!(e.to_string()))?;
        Ok(DataFileDetails::new_invalid())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_generator_wikipage() {
        Generator::wikipage("wikidatawiki","User:Magnus Manske/ToolFlow test", 4420).await.unwrap();
    }

}