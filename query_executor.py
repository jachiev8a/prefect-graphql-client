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
        "--print-main-general-report",
        action="store_true",
        required=False,
        help=(
            "Prints a general report of the current workflows filtered by project name. "
            "Report include schedule information (even if not set). "
            "This is the report with most details set."
        ),
    )
    parser.add_argument(
        "-p",
        "--project-filter",
        action="append",
        default=None,
        required=False,
        metavar="PROJECT_FILTER",
        help="filter output elements from the reports by project name. "
             "(e.g. -p production, -p development)",
    )

    args = parser.parse_args()
    arg_print_schedule_active = args.print_schedule_active
    arg_print_schedule_config = args.print_schedule_config
    arg_print_main_general_report = args.print_main_general_report
    arg_project_filter: list[str] = args.project_filter

    # validate that any of the important arguments are set.
    any_print_selected = (
        arg_print_schedule_active or
        arg_print_schedule_config or
        arg_print_main_general_report
    )

    if not any_print_selected:
        exit("ERROR: no option to print was selected!")

    prefect_api_key = config("PREFECT_API_KEY")
    prefect_tenant_id = config("PREFECT_TENANT_ID")

    client = PrefectCloudApiModel(
        api_key=prefect_api_key,
        tenant_id=prefect_tenant_id
    )

    if arg_print_schedule_active:
        client.print_report_schedule_active(project_filter=arg_project_filter.pop())
        print("")
        print("")

    if arg_print_schedule_config:
        client.print_report_schedule_configurations(
            project_filter=arg_project_filter.pop(),
            sort_by="schedule",
        )

    if arg_print_schedule_config:
        client.print_report_schedule_configurations(
            project_filter=arg_project_filter.pop(),
            sort_by="schedule",
        )

    if arg_print_main_general_report:
        client.print_general_report(
            project_filters=arg_project_filter,
            sort_by="schedule",
        )


if __name__ == "__main__":
    main()
