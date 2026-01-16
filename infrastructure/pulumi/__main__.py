"""
Portia infrastructure deployment with Pulumi.

Deploys:
1. MS FHIR Converter (internal Cloud Run service)
2. Portia API (Cloud Run service)
3. GCS buckets for temp storage and exports
"""

import pulumi
import pulumi_gcp as gcp

# Configuration
config = pulumi.Config()
gcp_config = pulumi.Config("gcp")

project_id = gcp_config.require("project")
region = gcp_config.require("region")

image_tag = config.get("imageTag") or "latest"
min_instances = int(config.get("minInstances") or "0")
max_instances = int(config.get("maxInstances") or "5")

# Enable required APIs
apis = [
    "run.googleapis.com",
    "storage.googleapis.com",
    "secretmanager.googleapis.com",
    "healthcare.googleapis.com",
    "artifactregistry.googleapis.com",
]

enabled_apis = []
for api in apis:
    service = gcp.projects.Service(
        f"enable-{api.split('.')[0]}",
        service=api,
        project=project_id,
        disable_on_destroy=False,
    )
    enabled_apis.append(service)

# Create Artifact Registry repository for Portia images
artifact_repo = gcp.artifactregistry.Repository(
    "portia-repo",
    repository_id="portia",
    format="DOCKER",
    location=region,
    project=project_id,
    opts=pulumi.ResourceOptions(depends_on=enabled_apis),
)


# Create service account for MS FHIR Converter
ms_converter_sa = gcp.serviceaccount.Account(
    "ms-converter-sa",
    account_id="ms-fhir-converter",
    display_name="MS FHIR Converter Service Account",
    project=project_id,
)

# Create service account for Portia
portia_sa = gcp.serviceaccount.Account(
    "portia-sa",
    account_id="portia-cloudrun",
    display_name="Portia Cloud Run Service Account",
    project=project_id,
)

# Grant IAM roles to Portia service account
portia_roles = [
    "roles/storage.objectAdmin",
    "roles/secretmanager.secretAccessor",
    "roles/healthcare.fhirStoreAdmin",
    "roles/logging.logWriter",
    "roles/cloudtrace.agent",
]

for i, role in enumerate(portia_roles):
    gcp.projects.IAMMember(
        f"portia-iam-{i}",
        project=project_id,
        role=role,
        member=portia_sa.email.apply(lambda e: f"serviceAccount:{e}"),
    )

# Create GCS buckets
temp_bucket = gcp.storage.Bucket(
    "portia-temp",
    name=f"{project_id}-portia-temp",
    location=region,
    uniform_bucket_level_access=True,
    lifecycle_rules=[
        gcp.storage.BucketLifecycleRuleArgs(
            action=gcp.storage.BucketLifecycleRuleActionArgs(type="Delete"),
            condition=gcp.storage.BucketLifecycleRuleConditionArgs(age=1),
        )
    ],
    opts=pulumi.ResourceOptions(depends_on=enabled_apis),
)

exports_bucket = gcp.storage.Bucket(
    "portia-exports",
    name=f"{project_id}-portia-exports",
    location=region,
    uniform_bucket_level_access=True,
    lifecycle_rules=[
        gcp.storage.BucketLifecycleRuleArgs(
            action=gcp.storage.BucketLifecycleRuleActionArgs(type="Delete"),
            condition=gcp.storage.BucketLifecycleRuleConditionArgs(age=7),
        )
    ],
    opts=pulumi.ResourceOptions(depends_on=enabled_apis),
)

# MS FHIR Converter is deployed via GitHub Actions (deploy-ms-converter job)
# because we need to copy the image from MCR to our Artifact Registry first.
# The service uses the ms_converter_sa service account created above.

# MS Converter URL will be constructed dynamically
ms_converter_url = pulumi.Output.concat(
    "https://ms-fhir-converter-",
    project_id,
    ".run.app",
)

# Cloud Run services are deployed via GitHub Actions after images are built
# Pulumi creates the infrastructure (buckets, IAM, Artifact Registry)
# GitHub Actions handles: build image -> push to AR -> deploy to Cloud Run

# Construct expected URLs for reference
portia_url = pulumi.Output.concat(
    "https://portia-",
    project_id,
    ".run.app",
)

# Export outputs
pulumi.export("portia_url", portia_url)
pulumi.export("ms_converter_url", ms_converter_url)
pulumi.export("portia_service_account", portia_sa.email)
pulumi.export("ms_converter_service_account", ms_converter_sa.email)
pulumi.export("temp_bucket", temp_bucket.name)
pulumi.export("exports_bucket", exports_bucket.name)
pulumi.export("artifact_repo", artifact_repo.name)
pulumi.export("project_id", project_id)
pulumi.export("region", region)
