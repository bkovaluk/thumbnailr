use aws_lambda_events::{event::s3::S3Event, s3::S3EventRecord};
use aws_sdk_s3::Client as S3Client;
use aws_config::BehaviorVersion;
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};
use s3::{GetFile, PutFile};

mod s3;

/**
This lambda handler
    * listen to file creation events
    * downloads the created file
    * creates a thumbnail from it
    * uploads the thumbnail to bucket "[original bucket name]-thumbs".

Make sure that
    * the created png file has no strange characters in the name
    * there is another bucket with "-thumbs" suffix in the name
    * this lambda only gets event from png file creation
    * this lambda has permission to put file into the "-thumbs" bucket
*/
pub(crate) async fn function_handler<T: PutFile + GetFile>(
    event: LambdaEvent<S3Event>,
    size: u32,
    client: &T,
) -> Result<(), Error> {
    let records = event.payload.records;

    for record in records.into_iter() {
        let (bucket, key) = match get_file_props(record) {
            Ok(touple) => touple,
            Err(msg) => {
                tracing::info!("Record skipped with reason: {}", msg);
                continue;
            }
        };

        let image = match client.get_file(&bucket, &key).await {
            Ok(vec) => vec,
            Err(msg) => {
                tracing::info!("Can not get file from S3: {}", msg);
                continue;
            }
        };

        let thumbnail = match get_thumbnail(image, size) {
            Ok(vec) => vec,
            Err(msg) => {
                tracing::info!("Can not create thumbnail: {}", msg);
                continue;
            }
        };

        let mut thumbs_bucket = bucket.to_owned();
        thumbs_bucket.push_str("-thumbs");

        // It uploads the thumbnail into a bucket name suffixed with "-thumbs"
        // So it needs file creation permission into that bucket

        match client.put_file(&thumbs_bucket, &key, thumbnail).await {
            Ok(msg) => tracing::info!(msg),
            Err(msg) => tracing::info!("Can not upload thumbnail: {}", msg),
        }
    }

    Ok(())
}

fn get_file_props(record: S3EventRecord) -> Result<(String, String), String> {
    record
        .event_name
        .filter(|s| s.starts_with("ObjectCreated"))
        .ok_or("Wrong event")?;

    let bucket = record
        .s3
        .bucket
        .name
        .filter(|s| !s.is_empty())
        .ok_or("No bucket name")?;

    let key = record.s3.object.key.filter(|s| !s.is_empty()).ok_or("No object key")?;

    Ok((bucket, key))
}

#[cfg(not(test))]
fn get_thumbnail(vec: Vec<u8>, size: u32) -> Result<Vec<u8>, String> {
    use std::io::Cursor;

    use thumbnailer::{create_thumbnails, ThumbnailSize};

    let reader = Cursor::new(vec);
    let mime = mime::IMAGE_PNG;
    let sizes = [ThumbnailSize::Custom((size, size))];

    let thumbnail = match create_thumbnails(reader, mime, sizes) {
        Ok(mut thumbnails) => thumbnails.pop().ok_or("No thumbnail created")?,
        Err(thumb_error) => return Err(thumb_error.to_string()),
    };

    let mut buf = Cursor::new(Vec::new());

    match thumbnail.write_png(&mut buf) {
        Ok(_) => Ok(buf.into_inner()),
        Err(_) => Err("Unknown error when Thumbnail::write_png".to_string()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // required to enable CloudWatch error logging by the runtime
    tracing::init_default_subscriber();

    let shared_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = S3Client::new(&shared_config);
    let client_ref = &client;

    let func = service_fn(move |event| async move { 
        function_handler(event, 128, client_ref).await 
    });

    run(func).await?;

    Ok(())
}

#[cfg(test)]
fn get_thumbnail(vec: Vec<u8>, _size: u32) -> Result<Vec<u8>, String> {
    let s = unsafe { std::str::from_utf8_unchecked(&vec) };

    match s {
        "IMAGE" => Ok("THUMBNAIL".into()),
        _ => Err("Input is not IMAGE".to_string()),
    }
}
