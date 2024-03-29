import os
import pathlib
from zoneinfo import ZoneInfo

from typing import Dict
from typing import List

from cron_descriptor import get_description, Options, CasingTypeEnum
from cron_converter import Cron
from decouple import config

import queries

# add backend path to environment
backend_abspath = os.path.join(pathlib.Path(__file__).parent, 'config', 'backend.toml')
os.environ["PREFECT__BACKEND_CONFIG_PATH"] = backend_abspath

# get values from env
LOCAL_TIMEZONE = config("LOCAL_TIMEZONE", default='localtime')
LOCAL_TIMEZONE_STR_FMT = "%I:%M %p"

pair_hours = "2,4,6,8,10,12,14,16,18,20,22"
odd_hours = "1,3,5,7,9,11,13,15,17,19,21,23"
minutes = "1,11,21,31,41,51"
split_minutes = [f"{x}" for x in minutes.split(',')]

cron_stack_pair = [f"{min} {pair_hours} * * *" for min in split_minutes]
cron_stack_odd = [f"{min} {odd_hours} * * *" for min in split_minutes]
CRON_STACK = cron_stack_pair + cron_stack_odd


# cron descriptor options
CRON_DESCRIPTOR_OPTIONS = Options()
CRON_DESCRIPTOR_OPTIONS.throw_exception_on_parse_error = True
CRON_DESCRIPTOR_OPTIONS.casing_type = CasingTypeEnum.Sentence
CRON_DESCRIPTOR_OPTIONS.use_24hour_time_format = True


class ScheduleClock(object):

    UTC_STR = "(UTC)"

    def __init__(self, clock_data):
        self.type = clock_data.get("type")
        self.value = clock_data.get("cron", "")
        self.parameters = self._get_parameters(clock_data)

    def _get_parameters(self, clock_data):
        raw_parameters = clock_data.get("parameter_defaults", {})
        parameters = {}
        for key, value in raw_parameters.items():
            parameters[key] = value
        return parameters

    def get_converted_datetime_from_cron_value(
        self,
        cron_value: str,
        timezone: str = "localtime"
    ):
        cron_schedule = Cron(cron_value).schedule()
        cron_datetime_utc = cron_schedule.next()
        converted_datetime = cron_datetime_utc.astimezone(ZoneInfo(timezone))
        return converted_datetime

    def get_human_description(self):
        if self.is_cron():
            # convert cron string value into human-readable string
            cron_human_description = get_description(
                expression=self.value,
                options=CRON_DESCRIPTOR_OPTIONS,
            )
            converted_datetime = self.get_converted_datetime_from_cron_value(
                cron_value=self.value,
                timezone=LOCAL_TIMEZONE
            )
            converted_datetime_str = converted_datetime.strftime(LOCAL_TIMEZONE_STR_FMT)
            return (
                f"{cron_human_description} {self.UTC_STR} --- "
                f"[ {converted_datetime_str} - ({LOCAL_TIMEZONE}) ]"
            )
        return "NA"

    def is_cron(self):
        return bool(self.type == "CronClock")

    def __str__(self):
        return f"{self.type} | {self.value} | Parameters: {self.parameters}"


class ProjectObject(object):

    def __init__(self, project_data):
        self.id = None
        self.name = None
        self._retrieve_values(project_data)

    def _retrieve_values(self, raw_data: Dict):
        self.id = raw_data.get("id")
        self.name = raw_data.get("name")

    def is_elec_amr(self):
        return bool("electricity-amr" in self.name)

    def is_elec_ami(self):
        return bool("electricity-ami" in self.name)

    def is_dev(self):
        return bool("dev" in self.name)

    def is_prod(self):
        return bool("prod" in self.name)


class FlowObject(object):
    VERSION_SEP = "::"

    def __init__(self, flow_data):
        self.id = None
        self.name = None
        self._is_schedule_active = None
        self.version = None
        self.project = None  # type: ProjectObject
        self._retrieve_values(flow_data)

    def _retrieve_values(self, raw_data: Dict):
        self.id = raw_data.get("id")
        self.name = raw_data.get("name")
        self._is_schedule_active = raw_data.get("is_schedule_active")
        self.version = raw_data.get("version")
        self.project = ProjectObject(raw_data.get("project"))

    def is_schedule_active(self):
        return bool(self._is_schedule_active)

    def get_versioned_name(self):
        return f"{self.name}{self.VERSION_SEP}V{self.version}"


class FlowGroupObject(object):

    def __init__(self, flow_group_data):
        self.name = None
        self.id = None
        self.labels = []
        self.schedules = []  # type: List[ScheduleClock]
        self.flows = []  # type: List[FlowObject]
        self.project = None  # type: ProjectObject or None
        self._retrieve_values(flow_group_data)

    def _retrieve_values(self, raw_data: Dict):
        self.name = raw_data.get("name")
        self.id = raw_data.get("id")
        self.labels = raw_data.get("labels")

        schedule_data = raw_data.get("schedule", {})
        if schedule_data:
            schedule_clocks = schedule_data.get("clocks", [])
            for clock_data in schedule_clocks:
                self.schedules.append(ScheduleClock(clock_data))

        flows = raw_data.get("flows", [])
        for flow_data in flows:
            self.flows.append(FlowObject(flow_data))
        self.flows.sort(key=lambda flow: int(flow.version), reverse=True)

        self.project = self.flows[0].project

    def get_latest_flow(self) -> FlowObject:
        if self.flows:
            return self.flows[0]


class ClauseWhere(object):
    class FilterField(object):
        TEMPLATE = "${field}: { ${field_name}: {_eq: \"${field_value}\" } }"

        def __init__(self, field, field_name, field_value):
            self.field = field
            self.field_name = field_name
            self.field_value = field_value

        def __str__(self):
            string = self.TEMPLATE
            string = string.replace('${field}', self.field)
            string = string.replace('${field_name}', self.field_name)
            string = string.replace('${field_value}', self.field_value)
            return string

    def __init__(self):
        self.filters = []

    def add_filtering(self, field, field_name, field_value):
        self.filters.append(
            str(self.FilterField(field, field_name, field_value))
        )

    def to_str(self):
        return self.__str__()

    def __str__(self):
        filters = ", ".join(self.filters)
        string = "where: { $_statement }".replace("$_statement", filters)
        return string


class ClauseOrderBy(object):

    def __init__(self, field_name, order_by='desc'):
        self.field_name = field_name
        self.order_by = order_by

    def to_str(self):
        return self.__str__()

    def __str__(self):
        string = "order_by: { " + f"{self.field_name}: {self.order_by}" + " }"
        return string


class GraphQlBaseQuery(object):
    def __init__(self, template: str):
        self.query = ""

    def __str__(self):
        return self.query


class GraphQlFlowGroupQuery(GraphQlBaseQuery):
    TAG_PROJECT_NAME = "$_PROJECT_NAME"
    TAG_ORDER_BY_FIELD = "$_TAG_ORDER_BY_FIELD"
    TAG_ORDER_BY_SEQUENCE = "$_TAG_ORDER_BY_TYPE"

    TAG_WHERE = "$_TAG_WHERE"
    TAG_ORDER_BY = "$_TAG_ORDER_BY"
    TAG_FLOW_GROUP_FIELDS = "$_TAG_FLOW_GROUP_FIELDS"

    TEMPLATE_WHERE = 'where: { project: {name: {_eq: "$_PROJECT_NAME"}} }'
    TEMPLATE_ORDER_BY = "order_by: {$_TAG_ORDER_BY_FIELD: $_TAG_ORDER_BY_TYPE}"

    QUERY_TEMPLATE = """
    {
        flow_group(
            $_TAG_WHERE
            $_TAG_ORDER_BY
        ) {
            $_TAG_FLOW_GROUP_FIELDS
        }
    }
    """

    def __init__(
            self,
            project_name,
            fields_list,
            order_by,
            order_by_desc: bool = True,
    ):
        super().__init__("")
        self.query = self.QUERY_TEMPLATE

        # if project_name:
        #     template_where = self.TEMPLATE_WHERE.replace(
        #         self.TAG_PROJECT_NAME, project_name
        #     )
        #     self.query = self.query.replace(self.TAG_WHERE, template_where)
        # else:
        #     self.query = self.query.replace(self.TAG_WHERE, "")
        self.query = self.query.replace(self.TAG_WHERE, "")

        if order_by:
            template_order_by = self.TEMPLATE_ORDER_BY.replace(
                self.TAG_ORDER_BY_FIELD, order_by
            ).replace(self.TAG_ORDER_BY_SEQUENCE, "desc" if order_by_desc else "asc")
            self.query = self.query.replace(self.TAG_ORDER_BY, template_order_by)
        else:
            self.query = self.query.replace(self.TAG_ORDER_BY, "")

        fields_as_str = "\n".join(fields_list)
        self.query = self.query.replace(self.TAG_FLOW_GROUP_FIELDS, fields_as_str)


class GraphQlFlowQuery(GraphQlBaseQuery):
    TAG_PROJECT_NAME = "$_PROJECT_NAME"
    TAG_ORDER_BY_FIELD = "$_TAG_ORDER_BY_FIELD"
    TAG_ORDER_BY_SEQUENCE = "$_TAG_ORDER_BY_TYPE"

    TAG_WHERE = "${TAG_WHERE}"
    TAG_ORDER_BY = "${TAG_ORDER_BY}"
    TAG_FLOW_FIELDS = "${TAG_FLOW_FIELDS}"

    TEMPLATE_ORDER_BY = "order_by: {$_TAG_ORDER_BY_FIELD: $_TAG_ORDER_BY_TYPE}"

    QUERY_TEMPLATE = """
    {
        flow(
            ${TAG_WHERE}
            ${TAG_ORDER_BY}
        ) {
            ${TAG_FLOW_FIELDS}
        }
    }
    """

    def __init__(
            self,
            project_name,
            fields_list,
            order_by,
            order_by_desc: bool = True,
    ):
        super().__init__("")
        self.query = self.QUERY_TEMPLATE

        if project_name:
            where_clause = ClauseWhere()
            where_clause.add_filtering('project', 'name', project_name)
            self.query = self.query.replace(self.TAG_WHERE, where_clause.to_str())
        else:
            self.query = self.query.replace(self.TAG_WHERE, "")

        if order_by:
            order = 'desc' if order_by_desc else 'asc'
            order_by_clause = ClauseOrderBy(order_by, order)
            self.query = self.query.replace(self.TAG_ORDER_BY, str(order_by_clause))
        else:
            self.query = self.query.replace(self.TAG_ORDER_BY, "")

        fields_as_str = "\n".join(fields_list)
        self.query = self.query.replace(self.TAG_FLOW_FIELDS, fields_as_str)


class PrefectCloudApiModel(object):
    FIELD_IS_SCHEDULE_ACTIVE = "is_schedule_active"

    REPORT_TITLE_WORKFLOW = "Workflow"
    REPORT_TITLE_PROJECT = "Project"
    REPORT_TITLE_ACTIVE = "Active"
    REPORT_TITLE_SCHEDULE_CONFIG = "Schedule (Config)"

    REPORT_SEPARATOR = 120

    SORT_NAME_KEY = "name"
    SORT_SCHEDULE_ACTIVE = "active"
    SORT_SCHEDULE_CONFIG = "schedule"

    def __init__(self, api_key: str = None, tenant_id: str = None):
        import prefect
        if api_key and tenant_id:
            self.client = prefect.Client(api_key=api_key, tenant_id=tenant_id)
        else:
            self.client = prefect.Client()

    def execute_raw_query(self, query):
        response = self.client.graphql(query)
        return response

    def query_flows(self, project_name, order_by_field="version"):
        fields_to_query = [
            "name",
            "id",
            "version",
            "is_schedule_active",
            "flow_group {id}",
        ]
        query = GraphQlFlowQuery(
            project_name=project_name,
            fields_list=fields_to_query,
            order_by=order_by_field,
        )
        print(query)
        response = self.client.graphql(query)
        return response

    def query_flow_groups(self, project_name, order_by_field="updated"):
        fields_to_query = [
            "id",
            "name",
            # 'labels',
            # 'schedule',
            # 'settings',
            "flows { name project { name } }",
        ]
        query = GraphQlFlowGroupQuery(
            project_name=project_name,
            fields_list=fields_to_query,
            order_by=order_by_field,
        )
        response = self.client.graphql(query)
        return response

    def activate_workflows_schedule_by_project(self, project_name):

        flow_group_ids = {}
        query = queries.Q_FLOWS_FROM_PROJECT.replace(
            "$_PROJECT_NAME", project_name
        )
        response = self.execute_raw_query(query)
        flows_data = response.get('data').get('flow')
        for flow in flows_data:
            flow_id = flow.get('id')
            flow_name = flow.get('name')

            # mutation =
            response = self.execute_raw_query(queries.M_ACTIVATE_SCHEDULE)
        a = 0

    def _print_report_separator(self):
        print("-" * self.REPORT_SEPARATOR)

    def print_report_schedule_active(self, project_filter: str = None):

        query = self._get_query_from_factory(project_filter, include_schedule_only=True)

        response = self.execute_raw_query(query)
        flow_group_data = response.get('data', {}).get('flow_group', {})

        self._print_report_separator()
        print(f"{self.REPORT_TITLE_WORKFLOW:<55} {self.REPORT_TITLE_PROJECT:<25} {self.REPORT_TITLE_SCHEDULE_CONFIG:<15}")
        self._print_report_separator()

        flow_groups_by_project = {}  # type: Dict[str, List]

        for flow_group in flow_group_data:

            flow_group_obj = FlowGroupObject(flow_group)
            if not flow_group_obj.schedules:
                continue

            first_clock = flow_group_obj.schedules[0]
            if not first_clock.is_cron():
                continue

            proj_name = flow_group_obj.project.name

            if proj_name in flow_groups_by_project.keys():
                flow_groups_by_project[proj_name].append(flow_group_obj)
            else:
                flow_groups_by_project[proj_name] = []
                flow_groups_by_project[proj_name].append(flow_group_obj)

        for flow_group_name, flow_group_objects in flow_groups_by_project.items():
            print("")
            print(f"> {flow_group_name}")
            for flow_group_object in flow_group_objects:
                first_clock = flow_group_object.schedules[0]
                latest_flow = flow_group_object.flows[0]  # type: FlowObject
                flow_name_with_v = f"|- {latest_flow.get_versioned_name()}"

                result = (
                    f"{flow_name_with_v:<55} "
                    f"{latest_flow.project.name:<25} "
                    f"{first_clock.get_human_description()}"
                )
                print(result)
        print("")
        self._print_report_separator()

    def sort_flow_groups_by_value(self, flow_group_list: List[FlowGroupObject], sort_value):
        if sort_value == self.SORT_NAME_KEY:
            flow_group_list.sort(key=lambda _flow_group: _flow_group.flows[0].name)
        elif sort_value == self.SORT_SCHEDULE_ACTIVE:
            flow_group_list.sort(
                key=lambda _flow_group: _flow_group.flows[0].is_schedule_active(), reverse=True
            )
        elif sort_value == self.SORT_SCHEDULE_CONFIG:
            flow_group_list.sort(
                key=lambda _flow_group:
                _flow_group.schedules[0].get_human_description() if _flow_group.schedules else "N/A"
            )

    def print_report_schedule_configurations(
        self,
        project_filter: str = None,
        sort_by: str = None,
    ):

        query = self._get_query_from_factory(project_filter, include_schedule_only=True)

        response = self.execute_raw_query(query)
        flow_group_data = response.get('data', {}).get('flow_group', {})

        self._print_common_report_header(sort_by)

        flow_groups_by_project = {}  # type: Dict[str, List]

        for flow_group in flow_group_data:
            flow_group_obj = FlowGroupObject(flow_group)
            if flow_group_obj.schedules:
                first_clock = flow_group_obj.schedules[0]
                if first_clock.is_cron():

                    proj_name = flow_group_obj.project.name

                    if proj_name in flow_groups_by_project.keys():
                        flow_groups_by_project[proj_name].append(flow_group_obj)
                    else:
                        flow_groups_by_project[proj_name] = []
                        flow_groups_by_project[proj_name].append(flow_group_obj)

        for flow_group_name, flow_group_objects in flow_groups_by_project.items():
            print("")
            print(f"> {flow_group_name}")

            if sort_by:
                self.sort_flow_groups_by_value(flow_group_objects, sort_by)

            for flow_group_object in flow_group_objects:
                first_clock = flow_group_object.schedules[0]  # type: ScheduleClock
                latest_flow = flow_group_object.flows[0]  # type: FlowObject
                flow_schedule_active = '[ YES ]' if latest_flow.is_schedule_active() else '[-]'
                flow_name_with_v = f"|- {latest_flow.get_versioned_name()}"

                result = (
                    f"{flow_name_with_v:<62} "
                    f"{flow_group_object.project.name:<25} "
                    f"{flow_schedule_active:<10} "
                    f"{first_clock.get_human_description()}"
                )
                print(result)
        print("")

    def print_general_report(
        self,
        project_filters: list[str] = None,
        sort_by: str = None,
    ):
        if project_filters is None:
            project_filters = []

        queries_to_execute = self._get_queries_to_execute(project_filters)

        flow_group_data_list = []

        for query in queries_to_execute:
            response = self.execute_raw_query(query)
            flow_group_data_from_response = response.get('data', {}).get('flow_group', {})
            flow_group_data_list.append(flow_group_data_from_response)

        self._print_common_report_header(sort_by)

        flow_groups_by_project = {}  # type: Dict[str, List[FlowGroupObject]]

        for flow_group_data in flow_group_data_list:
            for flow_group in flow_group_data:
                flow_group_instance = FlowGroupObject(flow_group)

                proj_name = flow_group_instance.project.name

                if proj_name in flow_groups_by_project.keys():
                    flow_groups_by_project[proj_name].append(flow_group_instance)
                else:
                    flow_groups_by_project[proj_name] = []
                    flow_groups_by_project[proj_name].append(flow_group_instance)

        for flow_group_name, flow_group_objects in flow_groups_by_project.items():
            print("")
            print(f"> {flow_group_name}")

            if sort_by:
                self.sort_flow_groups_by_value(flow_group_objects, sort_by)

            for flow_group_object in flow_group_objects:

                has_one_schedule_only = True if len(flow_group_object.schedules) == 1 else False
                has_more_than_one_schedule = True if len(flow_group_object.schedules) > 1 else False

                clock_human_description = "[!] - [Not Configured yet]"
                if has_one_schedule_only:
                    first_clock = flow_group_object.schedules[0]  # type: ScheduleClock
                    clock_human_description = first_clock.get_human_description()
                    if first_clock.parameters:
                        clock_human_description = ""
                elif has_more_than_one_schedule:
                    clock_human_description = ""

                latest_flow = flow_group_object.get_latest_flow()  # type: FlowObject
                flow_schedule_active = '[ YES ]' if latest_flow.is_schedule_active() else '[-]'
                flow_name_with_v = f"|- {latest_flow.get_versioned_name()}"

                result = (
                    f"{flow_name_with_v:<62} "
                    f"{flow_group_object.project.name:<25} "
                    f"{flow_schedule_active:<10} "
                    f"{clock_human_description}"
                )
                print(result)

                has_parameters = False
                for schedule in flow_group_object.schedules:
                    schedule_params = schedule.parameters
                    if schedule_params:
                        has_parameters = True
                        params_as_str = f"|---- [Parameters]: {schedule_params}"
                        print(
                            f"{params_as_str:<100}"
                            f"{schedule.get_human_description()}"
                        )
                if has_parameters:
                    print("|")
        print("")

    def _print_common_report_header(self, sort_value=""):
        # workflow name
        workflow_name_title = self.REPORT_TITLE_WORKFLOW
        if sort_value == self.SORT_NAME_KEY:
            workflow_name_title += " [*]"

        # schedule active
        schedule_active_title = self.REPORT_TITLE_ACTIVE
        if sort_value == self.SORT_SCHEDULE_ACTIVE:
            schedule_active_title += " [*]"

        # schedule active
        schedule_config_title = self.REPORT_TITLE_SCHEDULE_CONFIG
        if sort_value == self.SORT_SCHEDULE_CONFIG:
            schedule_config_title += " [*]"

        # print report header
        self._print_report_separator()
        print(
            f"{workflow_name_title:<62} "
            f"{self.REPORT_TITLE_PROJECT:<25} "
            f"{schedule_active_title:<10} "
            f"{schedule_config_title:<15}"
        )
        self._print_report_separator()

    def _get_queries_to_execute(
        self,
        project_filters: list[str] = None,
    ) -> list[str]:
        queries_to_execute = []

        for project_filter in project_filters:
            query = self._get_query_from_factory(
                project_filter=project_filter,
                include_schedule_only=False
            )
            queries_to_execute.append(query)

        if not project_filters:
            queries_to_execute.append(
                self._get_query_from_factory(include_schedule_only=False)
            )
        return queries_to_execute

    def _get_query_from_factory(
        self,
        project_filter: str = None,
        include_schedule_only: bool = False
    ) -> str:

        query = ""

        if include_schedule_only:
            query = queries.Q_ALL_SCHEDULED_FLOWS

        if project_filter and include_schedule_only:
            query = queries.Q_ALL_SCHEDULED_FLOWS_WITH_PROJECT_FILTER

        if project_filter and not include_schedule_only:
            query = queries.Q_ALL_FLOW_GROUPS_WITH_PROJECT_FILTER

        if project_filter:
            query = query.replace("$_PROJECT_NAME", project_filter)

        return query
