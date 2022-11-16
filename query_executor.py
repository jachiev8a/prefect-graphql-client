import argparse

from decouple import config

from models.PrefectCloudApiModel import PrefectCloudApiModel


# ---------------
# MAIN
# ---------------
def main():
    """Main Function"""

    help_description = (
        "pending doc..."
    )

    # Script Argument Parser
    parser = argparse.ArgumentParser(description=help_description)
    # parser.add_argument(
    #     "-e",
    #     "--email",
    #     default=None,
    #     required=False,
    #     metavar="EMAIL",
    #     help="email to be used to authenticate to 1password account",
    # )
    parser.add_argument(
        "-s",
        "--print-schedule-active",
        action="store_true",
        required=False,
        help=(
            "flag to print a report for all workflows with current schedule active."
        ),
    )
    parser.add_argument(
        "-c",
        "--print-schedule-config",
        action="store_true",
        required=False,
        help=(
            "flag to print a report for all schedule configurations. "
            "If a workflows has been configured with schedule, it will be shown here."
        ),
    )
    parser.add_argument(
        "-r",
        "--print-general-report",
        action="store_true",
        required=False,
        help=(
            "..........................."
        ),
    )

    args = parser.parse_args()
    print_schedule_active = args.print_schedule_active
    print_schedule_config = args.print_schedule_config
    print_general_report = args.print_general_report

    any_print_selected = (
        print_schedule_active or
        print_schedule_config or
        print_general_report
    )

    if not any_print_selected:
        exit("ERROR: no option to print was selected!")

    prefect_api_key = config("PREFECT_API_KEY")
    prefect_tenant_id = config("PREFECT_TENANT_ID")

    client = PrefectCloudApiModel(
        api_key=prefect_api_key,
        tenant_id=prefect_tenant_id
    )

    if print_schedule_active:
        client.print_report_schedule_active(project_filter="prod")
        print("")
        print("")

    if print_schedule_config:
        client.print_report_schedule_configurations(
            project_filter="prod",
            sort_by="schedule",
        )

    if print_schedule_config:
        client.print_report_schedule_configurations(
            project_filter="prod",
            sort_by="schedule",
        )

    if print_general_report:
        client.print_general_report(
            project_filter="prod",
            sort_by="schedule",
        )


if __name__ == "__main__":
    main()
