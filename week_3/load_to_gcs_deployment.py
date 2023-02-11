from prefect.deployments import Deployment 
from load_to_gcs_batch import load_to_gcs_batch

github_deployment = Deployment.build_from_flow(
    flow=load_to_gcs_batch,
    name='Load to GCS (batch)',
    parameters={
        'color': 'yellow',
        'year': 2021,
        'months': [1]
    }
)

github_deployment.apply()
