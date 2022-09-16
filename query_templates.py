

Q_FLOW_GROUP_FROM_PROJECT = """
{
  flow_group(
    where: {
      # schedule: {  _has_keys_all: true }
      flows: {
        project: {
          name: { _eq: "$_PROJECT_NAME" }
        }
      }
    }
    order_by: {created: desc} 
  ) {
    name
    id
    labels
    schedule
    flows { id name version project { id name } }
  }
}
"""

Q_ALL_SCHEDULED_WORKFLOWS = """
{
  flow_group(
    where: {
      flows: {
        is_schedule_active: { _eq: true }
      }
    }
    order_by: {created: desc}
  ) {
    name
    id
    labels
    schedule
    flows { id name version is_schedule_active project { id name } }
  }
}
"""

Q_ALL_SCHEDULED_CONFIGURATIONS = """
{
  flow_group(
    where: {
      schedule: { _has_keys_any: "clocks" }
    }
    order_by: {created: desc}
  ) {
    name
    id
    labels
    schedule
    flows { id name version is_schedule_active project { id name } }
  }
}
"""

Q_ALL_FLOW_GROUPS_FROM_PROJECT = """
{
  flow_group(
    where: {
      flows: {
        project: {
          name: { _eq: "$_PROJECT_NAME" }
        }
      }
    }
    order_by: {created: desc}
  ) {
    name
    id
    labels
    schedule
    flows { id name version is_schedule_active project { id name } }
  }
}
"""

Q_FLOWS_FROM_PROJECT = """
{
  flow(
    where: {
      project: {
        name: { _eq: "$_PROJECT_NAME" }
      }
    }
    order_by: {version: desc}
  ) {
    name
    id
    is_schedule_active
    version
    project { id name }
  }
}
"""

M_ACTIVATE_SCHEDULE = """
mutation {
  set_schedule_active(
    input: {
      flow_id: "$_FLOW_ID"
    }
  ) {
    success
  }
}
"""

M_SETUP_CRON_SCHEDULE = """
mutation {
  set_flow_group_schedule(
    input: {
      flow_group_id: "$_FLOW_GROUP_ID", 
      cron_clocks: [{cron: "$_FLOW_CRON"}]
    }
  ) {
    success
  }
}
"""
