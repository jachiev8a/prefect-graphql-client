
Q_ALL_SCHEDULED_FLOWS = """
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

Q_ALL_SCHEDULED_FLOWS_WITH_PROJECT_FILTER = """
{
  flow_group(
    where: {
      flows: {
        is_schedule_active: { _eq: true }
        project: { name: { _ilike: "%$_PROJECT_NAME%" } }
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
