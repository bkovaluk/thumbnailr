use async_trait::async_trait;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use lambda_runtime::tracing;

#[async_trait]
pub trait GetFile {
    async fn get_file(&self, bucket: &str, key: &str) -> Result<Vec<u8>, GetObjectError>;
}

#[async_trait]
pub trait PutFile {
    async fn put_file(&self, bucket: &str, key: &str, bytes: Vec<u8>) -> Result<String, String>;
}

#[async_trait]
impl GetFile for S3Client {
    async fn get_file(&self, bucket: &str, key: &str) -> Result<Vec<u8>, GetObjectError> {
        tracing::info!("get file bucket {}, key {}", bucket, key);

        let output = self.get_object().bucket(bucket).key(key).send().await;

        return match output {
            Ok(response) => {
                let bytes = response.body.collect().await.unwrap().to_vec();
                tracing::info!("Object is downloaded, size is {}", bytes.len());
                Ok(bytes)
            }
            Err(err) => {
                let service_err = err.into_service_error();
                let meta = service_err.meta();
                tracing::info!("Error from aws when downloding: {}", meta.to_string());
                Err(service_err)
            }
        };
    }
}

#[async_trait]
impl PutFile for S3Client {
    async fn put_file(&self, bucket: &str, key: &str, vec: Vec<u8>) -> Result<String, String> {
        tracing::info!("put file bucket {}, key {}", bucket, key);
        let bytes = ByteStream::new(vec.into());
        let result = self.put_object().bucket(bucket).key(key).body(bytes).send().await;

        match result {
            Ok(_) => Ok(format!("Uploaded a file with key {} into {}", key, bucket)),
            Err(err) => Err(err.into_service_error().meta().message().unwrap().to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::operation::get_object::GetObjectError;
    // use aws_sdk_s3::error::SdkError;
    use async_trait::async_trait;

    struct MockS3Client;
    #[async_trait]
    impl GetFile for MockS3Client {
        async fn get_file(&self, _bucket: &str, _key: &str) -> Result<Vec<u8>, GetObjectError> {
            Ok(vec![1, 2, 3, 4, 5])
        }
    }

    #[async_trait]
    impl PutFile for MockS3Client {
        async fn put_file(&self, _bucket: &str, _key: &str, _bytes: Vec<u8>) -> Result<String, String> {
            Ok("Mock put success".to_string())
        }
    }

    #[tokio::test]
    async fn test_get_file() {
        let client = MockS3Client {};
        let result = client.get_file("dummy_bucket", "dummy_key").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_put_file() {
        let client = MockS3Client {};
        let result = client.put_file("dummy_bucket", "dummy_key", vec![1, 2, 3]).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Mock put success");
    }
}