# Loomer Workflow Engine

```
ready/step.json --claim--> running/{worker}/step.json --success--> done/success/
                                           \--retry--> ready/<next-bucket>/
```

Steps are coordinated solely through Azure Data Lake Storage Gen2 semantics
(atomic rename and leases). Each step is processed at least once and must be
idempotent.

## Auth

Workers and the API authenticate to ADLS using a Service Principal (client id,
client secret, tenant id) via `ClientSecretCredential`. Set the environment
variables `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, and `AZURE_TENANT_ID` along
with `ADLS_ENDPOINT` and `ADLS_FS` for the target filesystem.

## Modules

* **loomer-core** – contracts and task SPI (`dev.qg.loomer.core`).
* **loomer-adls** – ADLS coordination utilities (`dev.qg.loomer.adls`).
* **worker** – minimal worker loop.
* **api** – Spring Boot REST API for runs.

## Limitations

* Delivery is *at-least-once*; tasks must handle replays.
* Idempotence is enforced per step using `out.json` presence.
