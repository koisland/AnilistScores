use polars::prelude::{df, CsvWriter, DataFrame, NamedFrom, SerWriter, Series};
use reqwest::Client;
use serde_json::{json, Value};
use std::env::args;
use std::fs::File;

const QUERY_USER_MEDIA_SCORE: &str = "
query ($username: String, $media: MediaType) {
  MediaListCollection (userName: $username, type: $media) {
    lists {
        name
        entries {
            mediaId,
            score
        }
    }
  }
}
";

#[derive(Debug, Clone)]
pub struct AnilistScores {
    pub list_type: String,
    pub anilist_id: Vec<i64>,
    pub user_score: Vec<i64>,
    pub global_avg_score: Vec<i64>,
}

impl AnilistScores {
    pub fn as_dataframe(&self) -> Result<DataFrame, String> {
        let df = df!(
            "list_type" => vec![self.list_type.clone(); self.user_score.len()],
            "anilist_id" => self.anilist_id.clone(),
            "user_score" => self.user_score.clone(),
            "global_avg_score" => self.global_avg_score.clone()
        );
        if let Ok(df) = df {
            Ok(df)
        } else {
            let err_msg = format!("Unable to save Anilist scores to dataframe: {:?}", df);
            Err(err_msg)
        }
    }

    pub fn to_csv(&self, fname: &str) {
        if let Ok(mut df_res) = self.as_dataframe() {
            if let Ok(output_fh) = File::create(fname) {
                CsvWriter::new(output_fh)
                    .has_header(true)
                    .finish(&mut df_res)
                    .unwrap_or_else(|_| panic!("Unable to save file to {fname}"))
            } else {
                println!("Unable to create file at {fname}.")
            }
        } else {
            println!("Unable to generate dataframe to save.")
        }
    }
}

#[tokio::main]
async fn run_query(client: &Client, json_query: Value) -> Result<serde_json::Value, &'static str> {
    // Make HTTP post request
    let resp = client
        .post("https://graphql.anilist.co/")
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .body(json_query.to_string())
        .send()
        .await
        .expect("Query failed.")
        .text()
        .await;
    // Get json
    if let Ok(resp) = resp {
        if let Ok(result) = serde_json::from_str(&resp) {
            Ok(result)
        } else {
            Err("Malformed query response. Cannot convert to str.")
        }
    } else {
        Err("Cannot retrieve query response text.")
    }
}

pub fn parse_entry_values(list_value: &Value) -> (Vec<i64>, Vec<i64>) {
    let mut entries: Vec<i64> = vec![];
    let mut scores: Vec<i64> = vec![];
    let entry_ids_scores = list_value.get("entries").and_then(|value| value.as_array());

    if let Some(watched_entries) = entry_ids_scores {
        for entry in watched_entries.iter() {
            if let (Some(id_val), Some(score_val)) = (entry.get("mediaId"), entry.get("score")) {
                if let (Some(id), Some(score)) = (id_val.as_i64(), score_val.as_i64()) {
                    entries.push(id);
                    scores.push(score);
                }
            }
        }
    }

    (entries, scores)
}

pub fn run_query_avg_scores(
    client: &Client,
    media: &str,
    media_ids: &[i64],
) -> Result<Vec<i64>, &'static str> {
    let media_field = "
    $alias: Media (id: $media_id, type: $media) {
        averageScore
    }
    ";
    let media_fields: String = media_ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            media_field
                .replace("$media_id", &id.to_string())
                .replace("$alias", &format!("query_{i}"))
        })
        .collect();

    let query_media_score: String = format!(
        "
    query ($media: MediaType) {{
        {media_fields}
    }}
    "
    );

    let user_media_query = json!(
        {
            "query": query_media_score,
            "variables": {"media": media}
        }
    );
    if let Ok(res) = run_query(client, user_media_query) {
        let avg_score_value = res.get("data").unwrap();

        let mut avg_scores: Vec<(String, i64)> = avg_score_value
            .as_object()
            .unwrap()
            .into_iter()
            .map(|(k, v)| {
                if let Some(score) = v.get("averageScore") {
                    let parsed_score = score.as_i64().unwrap_or(0);
                    (k.replace("query_", ""), parsed_score)
                } else {
                    (k.replace("query_", ""), 0)
                }
            })
            .collect();
        avg_scores.sort();

        let avg_scores_only = avg_scores.into_iter().map(|(_, score)| score).collect();
        Ok(avg_scores_only)
    } else {
        Err("Query failed.")
    }
}
pub fn get_anilist_scores(username: &str, media: &str) -> Vec<AnilistScores> {
    let client = Client::new();
    // Define query and variables
    let user_media_query = json!(
        {
            "query": QUERY_USER_MEDIA_SCORE,
            "variables": {"username": username, "media": media}
        }
    );

    let mut anilist_scores: Vec<AnilistScores> = vec![];

    if let Ok(query_res) = run_query(&client, user_media_query) {
        let media_lists = &query_res
            .get("data")
            .and_then(|value| value.get("MediaListCollection"))
            .and_then(|value| value.get("lists"))
            .expect("Media lists not found.");

        for list in media_lists.as_array().unwrap().iter() {
            if let Some(list_name) = list.get("name") {
                let list_type = list_name
                    .as_str()
                    .expect("Cannot coerce list name to string.");

                let entries = match list_type {
                    "Watching" => Ok(parse_entry_values(list)),
                    "Completed" => Ok(parse_entry_values(list)),
                    _ => Err(()),
                };

                if let Ok(parsed_entries) = entries {
                    let (entries, scores) = parsed_entries;

                    if let Ok(avg_scores) = run_query_avg_scores(&client, media, &entries) {
                        let aniscores = AnilistScores {
                            list_type: list_name.to_string().replace('"', ""),
                            anilist_id: entries,
                            user_score: scores,
                            global_avg_score: avg_scores,
                        };
                        anilist_scores.push(aniscores);
                    }
                }
            }
        }
    }
    anilist_scores
}
fn main() {
    let username = args().nth(1).expect("No Anilist username provided.");
    let media_type = args()
        .nth(2)
        .expect("No media type provided. (ANIME/MANGA)")
        .to_uppercase();
    let anilist_scores = get_anilist_scores(&username, &media_type);

    println!(
        "This script queries an Anilist profile and calculates a global average score.
    - score <= 0.99 indicates contrarian taste.
    - score = 1.0 indicates completely average taste.
    - score >= 1.1 indicates contrarian taste.
    "
    );

    for score in anilist_scores.iter() {
        let list_type = &score.list_type;
        let csv_fname = format!("anilist_{media_type}_{list_type}_score_{username}.csv");
        score.to_csv(&csv_fname);
        
        // Catch case where user use decimal scoring system. 4.9 instead of 49.
        let user_score_sum: i64 = score.user_score.iter().sum();
        let avg_score_sum: i64 = score.global_avg_score.iter().sum();

        println!(
            "Average-ness score for '{}' series: {}\n",
            score.list_type,
            (user_score_sum as f64 / avg_score_sum as f64)
        )
    }
}
