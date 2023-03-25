from typing import List

from app.utils.db.ddl import DDLHandler


class HypertableDDL(DDLHandler):
    def __init__(self, time_column_name: str = "ts", interval: str = None):
        self.time_column_name = time_column_name
        self.interval = interval

    def upgrade(self, schema_name: str, table_name: str) -> List[str]:
        commands = [f"SELECT create_hypertable('{table_name}','{self.time_column_name}')"]
        if self.interval:
            commands.append(f"SELECT add_retention_policy('{table_name}',INTERVAL '{self.interval}')")
        return commands

    def downgrade(self, schema_name: str, table_name: str) -> List[str]:
        commands = [f"SELECT remove_retention_policy('{table_name}', true);"]
        return commands

    def ddl_name_sql_query(self, schema_name: str, table_name: str) -> str:
        return f"""
        select CONCAT('hypertable_',d.hypertable_name,'_', d.column_name)
from timescaledb_information.hypertables h,	timescaledb_information.dimensions d
where h.hypertable_schema = d.hypertable_schema
and h.hypertable_name = d.hypertable_name
and h.hypertable_name = '{table_name}'
and d.column_name = '{self.time_column_name}'
and d.hypertable_schema = '{schema_name}'
        """

    def ddl_name_from_metadata(self, schema_name: str, table_name: str) -> str:
        return f"hypertable_{table_name}_{self.time_column_name}"


class PGTableRetentionPolicy(DDLHandler):
    # https://docs.timescale.com/api/latest/actions/add_job/
    def __init__(self, time_column_name: str = "last_ts", interval: int = 8035200):
        self.time_column_name = time_column_name
        self.interval = interval

    def upgrade(self, schema_name: str, table_name: str) -> List[str]:
        return ["""
        CREATE or replace PROCEDURE retention(job_id INT, config JSONB)
    LANGUAGE PLPGSQL AS
    $$
    declare
        t_table varchar := config->>'table';
        t_column varchar:= config->>'column';
        t_expiry integer:= config->>'expiry';
        query varchar;
    begin
        query := 'delete from '
        || quote_ident(t_table)
        || ' where EXTRACT(EPOCH FROM (now() - '
        ||quote_ident(t_column)
        ||'))>'
        || t_expiry;
        execute query;
        RAISE NOTICE 'Executed job % with query %', job_id, query;
    END
    $$;
    """, f""" 
    SELECT add_job('retention', '1D', config => '{{"table":"vehicle", 
    "column":"{self.time_column_name}", 
    "expiry":{self.interval} }}');
    """]

    def downgrade(self, schema_name: str, table_name: str) -> List[str]:
        return [f"""
select delete_job(job_id)  from timescaledb_information.jobs j 
where  j.proc_schema = '{schema_name}'
and j.proc_name  = 'retention'
and j.config->>'table' = '{table_name}';
"""]

    def ddl_name_sql_query(self, schema_name: str, table_name: str) -> str:
        return f"""select concat(j.proc_name,'_',j.config->>'table') 
        from timescaledb_information.jobs j 
where  j.proc_schema = '{schema_name}'
and j.config->>'table' = '{table_name}'
and j.proc_name  = 'retention' """

    def ddl_name_from_metadata(self, schema_name: str, table_name: str) -> str:
        return f"retention_{table_name}"
