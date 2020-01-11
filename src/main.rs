use failure::Error;
use futures::compat::Future01CompatExt;
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
) -> Result<S3List, Error> {
    let req = ListObjectsV2Request {
        bucket: bucket.clone(),
        delimiter: Some("/".to_owned()),
        prefix: path.clone(),
        continuation_token: token,
        max_keys: Some(2),
        ..Default::default()
    };
    let rs = client.list_objects_v2(req).compat().await?;

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

    Ok((common, contents, path, next_continuation_token))
}

async fn main_async() {
    let client = S3Client::new(Region::UsEast1);
    let bucket = "ander-test".to_owned();
    let path = None;

    let mut search_paths = vec![path];
    let mut full_dirs = Vec::new();
    let mut full_objects = Vec::new();
    let parallel = 8_usize;
    let mut tasks = Vec::new();

    while !search_paths.is_empty() {
        let slice_paths = search_paths.took(parallel - tasks.len());
        full_dirs.append(&mut slice_paths.clone());

        let mut new_tasks: Vec<_> = slice_paths
            .into_iter()
            .map(|prefix| list_dirs(&client, bucket.clone(), prefix, None))
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

        for (mut dirs, mut objects, prefix, token) in results.into_iter() {
            search_paths.append(&mut dirs);
            full_objects.append(&mut objects);
            if token.is_some() {
                tasks.push(list_dirs(&client, bucket.clone(), prefix, token));
            }
        }
    }

    println!("{:#?}", full_dirs);
    println!("{:#?}", full_objects);
}

fn main() {
    tokio_compat::run_std(main_async());
}

trait Took {
    fn took(&mut self, n: usize) -> Self;
}

impl<T> Took for Vec<T> {
    fn took(&mut self, n: usize) -> Self {
        let mut res = Vec::new();

        for _i in 0..n {
            let element = self.pop();
            if let Some(x) = element {
                res.push(x)
            } else {
                break;
            }
        }

        res
    }
}
