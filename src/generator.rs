use crate::{data_file::DataFileDetails, APP};
use anyhow::{anyhow, Result};
use mediawiki::api::Api;
use regex::RegexBuilder;

#[derive(Default, Clone, Debug)]
pub struct Generator {}

impl Generator {
    pub async fn wikipage(
        wiki_table: &str,
        wiki: &str,
        page: &str,
        user_id: usize,
    ) -> Result<DataFileDetails> {
        let server = APP
            .get_webserver_for_wiki(wiki)
            .ok_or_else(|| anyhow!("Could not find web server for {wiki}"))?;
        let url = format!("https://{server}/w/api.php");
        let mut api = Api::new(&url).await?;
        APP.add_user_oauth_to_api(&mut api, user_id).await?;

        let title = mediawiki::title::Title::new_from_full(page, &api);
        let mut page = mediawiki::page::Page::new(title);
        let before = match page.text(&api).await {
            Ok(wikitext) => wikitext,
            Err(mediawiki::MediaWikiError::Missing(_)) => "",
            Err(e) => return Err(anyhow!(e.to_string())),
        };

        // TODO replace old section
        let start = "<!--TOOLFLOW GENERATOR START-->";
        let end = "<!--TOOLFLOW GENERATOR END-->";
        let re = RegexBuilder::new(&format!(r"(?s){start}.*{end}"))
            .multi_line(true)
            .crlf(true)
            .build()?;
        let replace_with = format!("{start}\n{wiki_table}\n{end}\n");
        let after = if re.is_match(&before) {
            re.replace_all(&before, replace_with.to_owned()).to_string()
        } else {
            format!("{before}\n{replace_with}").trim().to_string()
        };

        if before != after && !cfg!(test) {
            // Only perform the edit if something has changed
            // Do not actually edit the page in testing, we know the Api crate works
            page.edit_text(&mut api, after, "ToolFlow generator edit")
                .await
                .map_err(|e| anyhow!(e.to_string()))?;
        }
        Ok(DataFileDetails::new_invalid())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_generator_wikipage() {
        // Not really a test...
        Generator::wikipage(
            "foobar",
            "wikidatawiki",
            "User:Magnus Manske/ToolFlow test",
            4420,
        )
        .await
        .unwrap();
    }
}
