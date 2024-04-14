# VectorLoader

1. Start embedding server

```
python3 ./embeddingServer/server.py
```

2. Start loading your documents!
```
go run . -nodeAddress <server_ip> -bucketName <bucket_name> -username <username> -password <password> -startIndex <start_doc_id> -endIndex <end_doc_id> -batchSize <batch_size_for_insert>
```
