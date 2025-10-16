# Docker Bake definition for building with GitHub Actions cache
# This is used by CI/CD workflows to enable layer caching

target "dev" {
  context = "./services/dev/"
  contexts = {
    workspace = "."
  }
  dockerfile = "Dockerfile"
  args = {
    UID = "${MY_UID}"
    GID = "${MY_GID}"
  }
  tags = ["langgraph-checkpoint-cassandra-dev"]
}
