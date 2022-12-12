# Onebeat Data-Engineer Home Assignment - Kafka flow

> Needed tools for the assignment: Prepare a docker-compose file to start a Redpanda environment - guide: https://docs.redpanda.com/docs/platform/quickstart/quick-start-docker/ 

1. Open a repository in GitHub and work on a different branch than master/main.
2. Use RedPanda (Kafka compatible) to generate the following data flow:
  - Producer → Produce the following message {‘date’: today}
  - Consumer + Producer → Get the message, calculate the date integers sum, for example: ‘2022-01-03’ results will be ‘10’. At the end produce the results message {‘date’: today, ‘sum’: result}
  - Consumer → Print the result message
  You are welcome to right it in any language you prefer.
3. Dockerize it.

Share the GitHub repository, configuration files you created, and a sample script to run the flow.
