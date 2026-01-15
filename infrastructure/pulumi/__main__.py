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

# Deploy MS FHIR Converter (internal-only)
ms_converter_service = gcp.cloudrunv2.Service(
    "ms-fhir-converter",
    name="ms-fhir-converter",
    location=region,
    project=project_id,
    template=gcp.cloudrunv2.ServiceTemplateArgs(
        service_account=ms_converter_sa.email,
        timeout="300s",
        scaling=gcp.cloudrunv2.ServiceTemplateScalingArgs(
            min_instance_count=0,
            max_instance_count=5,
        ),
        containers=[
            gcp.cloudrunv2.ServiceTemplateContainerArgs(
                image="mcr.microsoft.com/healthcareapis/fhir-converter:latest",
                resources=gcp.cloudrunv2.ServiceTemplateContainerResourcesArgs(
                    limits={"cpu": "2", "memory": "2Gi"},
                    cpu_idle=True,
                ),
                ports=[
                    gcp.cloudrunv2.ServiceTemplateContainerPortArgs(
                        container_port=8080,
                    )
                ],
            )
        ],
    ),
    traffics=[
        gcp.cloudrunv2.ServiceTrafficArgs(
            percent=100,
            type="TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST",
        )
    ],
    ingress="INGRESS_TRAFFIC_INTERNAL_ONLY",
    opts=pulumi.ResourceOptions(depends_on=enabled_apis),
)

# Allow Portia to invoke MS Converter
gcp.cloudrunv2.ServiceIamMember(
    "ms-converter-invoker",
    project=project_id,
    location=region,
    name=ms_converter_service.name,
    role="roles/run.invoker",
    member=portia_sa.email.apply(lambda e: f"serviceAccount:{e}"),
)

# Build Portia image URL
portia_image = pulumi.Output.concat(
    region,
    "-docker.pkg.dev/",
    project_id,
    "/portia/portia:",
    image_tag,
)

# Deploy Portia service
portia_service = gcp.cloudrunv2.Service(
    "portia",
    name="portia",
    location=region,
    project=project_id,
    template=gcp.cloudrunv2.ServiceTemplateArgs(
        service_account=portia_sa.email,
        max_instance_request_concurrency=50,
        timeout="300s",
        scaling=gcp.cloudrunv2.ServiceTemplateScalingArgs(
            min_instance_count=min_instances,
            max_instance_count=max_instances,
        ),
        containers=[
            gcp.cloudrunv2.ServiceTemplateContainerArgs(
                image=portia_image,
                resources=gcp.cloudrunv2.ServiceTemplateContainerResourcesArgs(
                    limits={"cpu": "1", "memory": "1Gi"},
                    cpu_idle=True,
                ),
                envs=[
                    gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                        name="GCP_PROJECT_ID",
                        value=project_id,
                    ),
                    gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                        name="GCP_REGION",
                        value=region,
                    ),
                    gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                        name="MS_CONVERTER_URL",
                        value=ms_converter_service.uri,
                    ),
                    gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                        name="TEMP_BUCKET",
                        value=temp_bucket.name,
                    ),
                    gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                        name="EXPORTS_BUCKET",
                        value=exports_bucket.name,
                    ),
                ],
            )
        ],
    ),
    traffics=[
        gcp.cloudrunv2.ServiceTrafficArgs(
            percent=100,
            type="TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST",
        )
    ],
    ingress="INGRESS_TRAFFIC_ALL",
    opts=pulumi.ResourceOptions(
        depends_on=[enabled_apis, ms_converter_service, artifact_repo]
    ),
)

# Allow authenticated access to Portia
gcp.cloudrunv2.ServiceIamMember(
    "portia-invoker",
    project=project_id,
    location=region,
    name=portia_service.name,
    role="roles/run.invoker",
    member="allUsers",
)

# Export outputs
pulumi.export("portia_url", portia_service.uri)
pulumi.export("ms_converter_url", ms_converter_service.uri)
pulumi.export("portia_service_account", portia_sa.email)
pulumi.export("temp_bucket", temp_bucket.name)
pulumi.export("exports_bucket", exports_bucket.name)
pulumi.export("artifact_repo", artifact_repo.name)
