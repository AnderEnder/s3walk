use crossbeam_channel::{unbounded, Receiver, Sender};
use failure::Error;
use futures::future::join_all;
use rusoto_core::Region;
use rusoto_s3::*;
use std::process::exit;

type S3List = (
    Vec<Option<String>>,
    Vec<Object>,
    Option<String>,
    Option<String>,
);

async fn list_dirs(
    client: &S3Client,
    bucket: String,
    path: Option<String>,
    token: Option<String>,
    sender: Sender<Option<String>>,
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

    for prefix in common.iter() {
        if let Some(p) = prefix {
            sender.send(Some(p.to_owned())).unwrap();
        }
    }

    Ok((common, contents, path, next_continuation_token))
}

#[tokio::main]
async fn main() {
    let client = S3Client::new(Region::UsEast1);
    let bucket = "ander-test".to_owned();
    let path = None;

    let (sender, receiver) = unbounded::<Option<String>>();
    sender.send(path.clone()).unwrap();

    let mut full_objects = Vec::new();
    let parallel = 8_usize;
    let mut tasks = Vec::new();

    while !receiver.is_empty() || tasks.len() != 0 {
        let mut new_tasks: Vec<_> = (0..(parallel - tasks.len()))
            .filter_map(|_| match receiver.try_recv() {
                Ok(path) => Some(list_dirs(
                    &client,
                    bucket.clone(),
                    path,
                    None,
                    sender.clone(),
                )),
                _ => None,
            })
            .collect();

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

        for (mut _dirs, mut objects, prefix, token) in results.into_iter() {
            full_objects.append(&mut objects);
            if token.is_some() {
                tasks.push(list_dirs(
                    &client,
                    bucket.clone(),
                    prefix,
                    token,
                    sender.clone(),
                ));
            }
        }
    }

    for object in full_objects {
        println!("{}", object.key.unwrap());
    }
}
