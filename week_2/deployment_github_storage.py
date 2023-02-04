from prefect.deployments import Deployment 
from prefect.filesystems import GitHub
from load_to_gcs import load_to_gcs

github_block = GitHub.load("github-creds")

github_deployment = Deployment.build_from_flow(
    flow=load_to_gcs,
    name='Load to GCS (github)',
    storage=github_block
)

github_deployment.apply()
