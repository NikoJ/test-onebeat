To start the project, you need to run the command from the project root: ```docker-compose up -d```

Please wait 30-50 seconds

The output will be written to consumer_app logs: ```docker logs consumer_app 2>&1 | grep Result```

```
(base) npotapov@npotapov:~/load/test-onebeat$ docker logs consumer_app 2>&1 | grep Result
INFO: 13-Dec-22 07:59:59 - Result: {'date': '2022-12-13', 'sum': '13'}
```