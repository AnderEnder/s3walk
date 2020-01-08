use failure::Error;
use futures::compat::Future01CompatExt;
use futures::future::join_all;
use futures::join;
use rusoto_core::Region;
use rusoto_s3::*;

async fn dirs(
    client: &S3Client,
    bucket: String,
    path: Option<String>,
) -> Result<Vec<Option<String>>, Error> {
    let mut dirs = Vec::new();
    let mut files = Vec::new();

    let mut token = None;

    loop {
        let req = ListObjectsV2Request {
            bucket: bucket.clone(),
            delimiter: Some("/".to_owned()),
            prefix: path.clone(),
            continuation_token: token.clone(),
            ..Default::default()
        };
        let rs = client.list_objects_v2(req).compat().await?;

        let ListObjectsV2Output {
            common_prefixes,
            contents,
            continuation_token,
            ..
        } = rs;

        let mut common: Vec<_> = common_prefixes
            .unwrap_or_else(|| Vec::new())
            .into_iter()
            .map(|x| {
                let CommonPrefix { prefix } = x;
                prefix
            })
            .collect();
        let mut contents = contents.unwrap_or_else(|| Vec::new());

        dirs.append(&mut common);
        files.append(&mut contents);

        if continuation_token.is_none() {
            break;
        } else {
            token = continuation_token;
        }
    }

    // let files = contents.unwrap_or_else(|| Vec::new());

    Ok(dirs)
}

type S3List = (Vec<Option<String>>, Vec<Object>, Option<String>);

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
        continuation_token: token.clone(),
        ..Default::default()
    };
    let rs = client.list_objects_v2(req).compat().await?;

    let ListObjectsV2Output {
        common_prefixes,
        contents,
        continuation_token,
        ..
    } = rs;

    let common: Vec<_> = common_prefixes
        .unwrap_or_else(|| Vec::new())
        .into_iter()
        .map(|x| {
            let CommonPrefix { prefix } = x;
            prefix
        })
        .collect();

    let contents = contents.unwrap_or_else(|| Vec::new());

    Ok((common, contents, continuation_token))
}

async fn main_async() {
    let client = S3Client::new(Region::UsEast1);
    let bucket = "ander-test".to_owned();
    let path = None;
    let mut search_paths: Vec<Option<String>> = vec![path];
    let mut full_dirs = Vec::new();

    while search_paths.len() > 0 {
        let slice_paths: Vec<_> = search_paths.took(4);

        full_dirs.append(&mut slice_paths.clone());

        let future_paths: Vec<_> = slice_paths
            .into_iter()
            .map(|x| dirs(&client, bucket.clone(), x))
            .collect();

        let dirs: Vec<_> = join_all(future_paths)
            .await
            .into_iter()
            .map(|x| x.unwrap())
            .collect();

        for mut path_set in dirs.into_iter() {
            search_paths.append(&mut path_set);
        }
    }

    println!("{:#?}", full_dirs);
}

fn main() {
    tokio_compat::run_std(main_async());
}

trait TookVector {
    fn took(&mut self, n: usize) -> Self;
}

impl<T> TookVector for Vec<T> {
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
