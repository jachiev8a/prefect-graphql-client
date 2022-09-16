from decouple import config

from models.PrefectCloudApiModel import PrefectCloudApiModel

PREFECT_API_KEY = config("PREFECT_API_KEY")
PREFECT_TENANT_ID = config("PREFECT_TENANT_ID")

client = PrefectCloudApiModel(
    api_key=PREFECT_API_KEY,
    tenant_id=PREFECT_TENANT_ID
)

client.print_report_schedule_active()
print("")
print("")
client.print_report_schedule_configurations(sort_by="schedule")
