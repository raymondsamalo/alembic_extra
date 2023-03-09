from typing import List

from app.utils.db.ddl import DDLHandler

class HypertableDDL(DDLHandler):
    def __init__(self, time_column_name: str = "ts"):
        self.time_column_name = time_column_name

    def upgrade(self, schema_name: str, table_name: str) -> List[str]:
        commands = [f"SELECT create_hypertable('{table_name}','{self.time_column_name}')"]
        return commands

    def downgrade(self, schema_name: str, table_name: str) -> List[str]:
        commands = []
        return commands

    def ddl_name_sql_query_from_schema(self, schema_name: str) -> str:
        return f"""
        select CONCAT('hypertable_',d.hypertable_name,'_', d.column_name)
from timescaledb_information.hypertables h,	timescaledb_information.dimensions d
where h.hypertable_schema = d.hypertable_schema
and h.hypertable_name = d.hypertable_name
and d.hypertable_schema = '{schema_name}'
        """

    def ddl_name_from_metadata(self, schema_name: str, table_name: str) -> str:
        return f"hypertable_{table_name}_{self.time_column_name}"


class HypertableRetentionDDL(DDLHandler):
    def __init__(self, interval: str):
        self.interval = interval

    def upgrade(self, schema_name: str, table_name: str) -> List[str]:
        commands = [f"SELECT add_retention_policy('{table_name}',INTERVAL '{self.interval}')"]
        return commands

    def downgrade(self, schema_name: str, table_name: str) -> List[str]:
        commands = [f"SELECT remove_retention_policy('{table_name}');"]
        return commands

    def ddl_name_sql_query_from_schema(self, schema_name: str) -> str:
        return f""" select CONCAT('retention_policy_',d.hypertable_name)
from timescaledb_information.jobs d 
where d.hypertable_schema = '{schema_name}' and d.proc_name='policy_retention'"""

    def ddl_name_from_metadata(self, schema_name: str, table_name: str) -> str:
        return f"retention_policy_{table_name}"
