use failure::Error;
use futures::future::join_all;
use rusoto_core::Region;
use rusoto_s3::*;
use std::process::exit;
use tokio::sync::mpsc::{channel, Receiver, Sender};

type S3List = (Option<String>, Option<String>);

async fn list_dirs(
    client: &S3Client,
    bucket: String,
    path: Option<String>,
    token: Option<String>,
    mut sender: Sender<Option<String>>,
    mut key_sender: Sender<Object>,
) -> Result<S3List, Error> {
    let req = ListObjectsV2Request {
        bucket: bucket.clone(),
        delimiter: Some("/".to_owned()),
        prefix: path.clone(),
        continuation_token: token,
        max_keys: Some(2),
        ..Default::default()
    };
    let rs = client.list_objects_v2(req).await?;

    let ListObjectsV2Output {
        common_prefixes,
        contents,
        next_continuation_token,
        ..
    } = rs;

    let common: Vec<_> = common_prefixes
        .unwrap_or_else(Vec::new)
        .into_iter()
        .map(|x| {
            let CommonPrefix { prefix } = x;
            prefix
        })
        .collect();

    let contents = contents.unwrap_or_else(Vec::new);

    for prefix in common {
        if let Some(p) = prefix {
            sender.send(Some(p.to_owned())).await?;
        }
    }

    for key in contents {
        key_sender.send(key).await?;
    }

    Ok((path, next_continuation_token))
}

async fn execute(mut receiver: Receiver<Object>) {
    while let Some(key) = receiver.recv().await {
        println!("{}", key.key.unwrap_or_else(|| "".to_owned()));
    }
}

#[tokio::main]
async fn main() {
    let client = S3Client::new(Region::UsEast1);
    let bucket = "ander-test".to_owned();
    let path = None;

    let (sender, mut receiver) = channel::<Option<String>>(1000);
    let (key_send, key_recv) = channel::<Object>(10000);

    tokio::spawn(execute(key_recv));

    let parallel = 8_usize;
    let mut tasks = vec![list_dirs(
        &client,
        bucket.clone(),
        path,
        None,
        sender.clone(),
        key_send.clone(),
    )];

    'main: loop {
        let mut new_tasks = Vec::new();

        for _ in 0..(parallel - tasks.len()) {
            match receiver.try_recv() {
                Ok(path) => {
                    let task = list_dirs(
                        &client,
                        bucket.clone(),
                        path,
                        None,
                        sender.clone(),
                        key_send.clone(),
                    );
                    new_tasks.push(task);
                }
                _ => {
                    if tasks.is_empty() && new_tasks.is_empty() {
                        break 'main;
                    }
                }
            };
        }

        new_tasks.append(&mut tasks);

        let results: Vec<_> = join_all(new_tasks)
            .await
            .into_iter()
            .filter_map(|x| {
                x.map_err(|e| {
                    eprintln!("{}", e);
                    exit(1);
                })
                .ok()
            })
            .collect();

        for (prefix, token) in results {
            if token.is_some() {
                let task = list_dirs(
                    &client,
                    bucket.clone(),
                    prefix,
                    token,
                    sender.clone(),
                    key_send.clone(),
                );
                tasks.push(task);
            }
        }
    }
}
