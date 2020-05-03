use failure::Error;
use futures::stream::{self, Stream, StreamExt};
use regex::Regex;
use rusoto_core::Region;
use rusoto_s3::*;
use std::str::FromStr;
use structopt::clap::AppSettings;
use structopt::StructOpt;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[derive(StructOpt, Debug, Clone)]
#[structopt(
    name = "s3walk",
    global_settings(&[AppSettings::ColoredHelp, AppSettings::NeedsLongHelp, AppSettings::NeedsSubcommandHelp])
)]
struct Opts {
    /// S3 url, example s3://bucket/path
    #[structopt(name = "path")]
    path: S3Path,

    /// Level concurrency to speedup
    #[structopt(name = "level", long = "concurrency", default_value = "8")]
    concurrent: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct S3Path {
    pub bucket: String,
    pub prefix: Option<String>,
}

const PARSE_ERROR: &str = "Cannnot parse s3 uri";

impl FromStr for S3Path {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let regex =
            Regex::new(r#"s3://([\d\w _-]+)(/([\d\w/ _-]*))?"#).map_err(|x| x.to_string())?;
        let captures = regex.captures(s).ok_or(PARSE_ERROR)?;

        let bucket = captures
            .get(1)
            .map(|x| x.as_str().to_owned())
            .ok_or(PARSE_ERROR)?;
        let prefix = captures.get(3).map(|x| x.as_str().to_owned());

        Ok(S3Path { bucket, prefix })
    }
}

fn print(items: Option<Vec<Object>>) {
    if let Some(list) = items {
        //println!("{:?}", list);
        list.into_iter()
            .for_each(|object| println!("{}", object.key.unwrap_or_else(|| "".to_owned())))
    }
}

async fn stream_walk_prefix(
    client: S3Client,
    bucket: String,
    path: Option<String>,
    sender: Sender<Option<String>>,
) -> impl Stream<Item = Option<Vec<Object>>> {
    let token = None;
    let stream = stream::unfold(
        (client, bucket, path, false, token, sender),
        |(client, bucket, path, last, token, sender)| async move {
            if last {
                return None;
            }

            let req = ListObjectsV2Request {
                bucket: bucket.clone(),
                delimiter: Some("/".to_owned()),
                prefix: path.clone(),
                continuation_token: token.clone(),
                max_keys: Some(2),
                ..Default::default()
            };

            let rs = client.list_objects_v2(req).await.unwrap();

            //println!("{:?}", &rs);
            let ListObjectsV2Output {
                next_continuation_token,
                common_prefixes,
                contents,
                ..
            } = rs.clone();

            if let Some(prefixes) = common_prefixes {
                let mut s = sender.clone();
                for prefix in prefixes.into_iter() {
                    s.send(prefix.prefix).await.unwrap();
                }
            }

            let state = (
                client,
                bucket,
                path,
                next_continuation_token.is_none(),
                next_continuation_token,
                sender,
            );
            Some((contents, state))
        },
    );
    stream
}

async fn stream_walk(
    client: S3Client,
    bucket: String,
    receiver: Receiver<Option<String>>,
    sender: Sender<Option<String>>,
) -> impl Stream<Item = impl Stream<Item = Option<Vec<Object>>>> {
    let stream = stream::unfold(
        (client, bucket, receiver, sender),
        |(client, bucket, mut receiver, sender)| async move {
            if let Some(key) = receiver.recv().await {
                let stream =
                    stream_walk_prefix(client.clone(), bucket.clone(), key, sender.clone()).await;
                let state = (client, bucket, receiver, sender);
                Some((stream, state))
            } else {
                None
            }
        },
    );
    stream
}

#[tokio::main]
async fn main() {
    let opts = Opts::from_args();

    let client = S3Client::new(Region::UsEast1);
    let S3Path { bucket, prefix } = opts.path;

    let (mut sender, receiver) = channel::<Option<String>>(1000);
    sender.send(prefix.clone()).await.unwrap();

    stream_walk(client, bucket, receiver, sender)
        .await
        .for_each_concurrent(opts.concurrent, |st| async move {
            st.map(print).collect::<Vec<_>>().await;
        })
        .await;
}
