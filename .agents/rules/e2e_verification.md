# Local End-to-End (E2E) Container Testing Rule

- **Mandatory E2E Verification**: Whenever modifying, creating, or packaging containerized microservices or deployment artifacts (e.g. Dockerfile, Fargate, Kubernetes, serverless platforms), you MUST run a local containerized E2E test (`make e2e-test` or equivalent local docker runner) to verify startup, health checks, REST APIs, and static asset serving BEFORE declaring tasks completed or deploying to cloud environments.
- **Fail-Safe Fallbacks**: Ensure serverless and cloud storage drivers (e.g. S3) fall back gracefully to local/in-memory drivers when environment credentials are not present, preventing container startup crashes (`CrashLoopBackOff`).
- **Clean Context & Toolchain**: Ensure `.dockerignore` excludes heavy local dependencies (`.terraform`, `node_modules`, build binaries) and Go module versions match container build toolchains (`GOTOOLCHAIN=auto`).
