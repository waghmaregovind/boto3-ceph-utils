from dataclasses import dataclass


@dataclass
class Config:
    endpoint_url: str = '<endpoint_url>'
    aws_access_key_id: str = '<aws_access_key_id>'
    aws_secret_access_key: str = '<aws_secret_access_key>'
    service_name: str = 's3'
    bucket_name: str = 'bucket_name'
    verify: bool = False