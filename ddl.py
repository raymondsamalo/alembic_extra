import json
import logging
from abc import abstractmethod, ABC
from typing import Tuple, Any, List

from alembic.autogenerate import comparators
from alembic.autogenerate import renderers
from alembic.operations import Operations, MigrateOperation
from sqlalchemy import DDL, text
from sqlmodel import SQLModel

logger = logging.getLogger(__name__)


class DDLHandler(ABC):
    __schema_name__: str  # assigned by create_ddl
    __table_name__: str  # assigned by create_ddl
    @abstractmethod
    def upgrade(self, schema_name: str, table_name: str) -> List[str]:
        """
        array of sql commands to install object
        :param schema_name:
        :param table_name:
        :return:
        """
        pass

    @abstractmethod
    def downgrade(self, schema_name: str, table_name: str) -> List[str]:
        """
        array of sql commands to remove object
        :param schema_name:
        :param table_name:
        :return:
        """
        pass

    @abstractmethod
    def ddl_name_sql_query(self, schema_name: str, table_name: str) -> str:
        """
        Query database to see if there is ddl_name in db
        :param schema_name:
        :param table_name:
        :return: SQL query that return 1 ddl_name per matching row
        """
        pass

    @abstractmethod
    def ddl_name_from_metadata(self, schema_name: str, table_name: str) -> str:
        """
        produce the same ddl_name as ddl_name_sql_query_from_schema for schema and table
        this is used to detect whether object exist in database for upgrade and downgrade
        :param schema_name:
        :param table_name:
        :return: unique name that mark this ddl
        """
        pass


ddl_registry: dict[str, DDLHandler] = {}


def create_ddl(model: SQLModel, ddl_handler: DDLHandler):
    schema_name = model.__table__.schema
    table_name = model.__tablename__
    ddl_name = ddl_handler.ddl_name_from_metadata(schema_name, table_name)
    model.metadata.info.setdefault("ddl", list()).append(
            {
                "schema": schema_name,
                "table": table_name,
                "ddl_name": ddl_name
            }
    )
    ddl_handler.__table_name__ = table_name
    ddl_handler.__schema_name__ = schema_name
    ddl_registry[ddl_name] = ddl_handler


@Operations.register_operation("add_ddl")
class AddDDLOp(MigrateOperation):
    def to_diff_tuple(self) -> Tuple[Any, ...]:
        pass

    def __init__(self, ddl_name, schema, hypertable_name):
        self.table_name = hypertable_name
        self.schema = schema
        self.ddl_name = ddl_name

    @classmethod
    def add_ddl(cls, operations, ddl_name, schema, hypertable_name, **kw):
        op = AddDDLOp(ddl_name, schema, hypertable_name, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return DropDDLOp(self.ddl_name, self.schema, self.table_name)


@Operations.register_operation("drop_ddl")
class DropDDLOp(MigrateOperation):

    def to_diff_tuple(self) -> Tuple[Any, ...]:
        pass

    def __init__(self, ddl_name, schema, table_name):
        self.table_name = table_name
        self.schema = schema
        self.ddl_name = ddl_name

    @classmethod
    def drop_ddl(cls, operations, ddl_name, schema, hypertable_name, **kw):
        op = DropDDLOp(ddl_name, schema, hypertable_name, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return AddDDLOp(self.ddl_name, self.table_name, self.schema)


@Operations.implementation_for(AddDDLOp)
def add_ddl(operations, operation):
    if operation.schema is None:
        schema = 'public'
    else:
        schema = operation.schema
    ddl_name = operation.ddl_name
    ddl_object = ddl_registry[ddl_name]
    for ddl_statement in ddl_object.upgrade(schema, operation.table_name):
        operations.execute(DDL(ddl_statement))


@Operations.implementation_for(DropDDLOp)
def drop_ddl(operations, operation):
    if operation.schema is None:
        schema = 'public'
    else:
        schema = operation.schema
    ddl_name = operation.ddl_name
    ddl_object = ddl_registry[ddl_name]
    for ddl_statement in ddl_object.downgrade(schema, operation.table_name):
        operations.execute(DDL(ddl_statement))


@renderers.dispatch_for(AddDDLOp)
def render_add_ddl(autogen_context, op):
    if op.schema:
        return "op.add_ddl(%r, %r, %r)" % (op.ddl_name, op.schema, op.table_name)
    return "op.add_ddl(%r, %r, %r)" % (op.ddl_name, 'public', op.table_name,)


@renderers.dispatch_for(DropDDLOp)
def render_drop_ddl(autogen_context, op):
    if op.schema:
        return "op.drop_ddl(%r, %r, %r)" % (op.ddl_name, op.schema, op.table_name)
    return "op.drop_ddl(%r, %r, %r)" % (op.ddl_name, 'public', op.table_name,)


@comparators.dispatch_for("schema")
def compare_ddl(autogen_context, upgrade_ops, schemas):
    logger.info('compare_ddl')
    metadata_in_database = set()

    for sch in schemas:
        schema_name = autogen_context.dialect.default_schema_name if sch is None else sch
        for ddl_name, ddl_handler in ddl_registry.items():
            table_name = ddl_handler.__table_name__
            statement = ddl_handler.ddl_name_sql_query(schema_name, table_name)
            result = autogen_context.connection.execute(text(statement))
            if ddl_handler.__schema_name__ is not None:
                metadata_in_database.update([(row[0], schema_name, table_name) for row in result])
            else:
                metadata_in_database.update([(row[0], None, table_name) for row in result])

    metadata_table_list = autogen_context.metadata.info.setdefault(
        "ddl", list())

    metadata_hypertable = set([(m["ddl_name"], m["schema"], m["table"]) for m in metadata_table_list])
  
    # for new names, produce CreateSequenceOp directives
    for ddl_name, sch, table in metadata_hypertable.difference(metadata_in_database):
        upgrade_ops.ops.append(
            AddDDLOp(ddl_name, sch, table)
        )

    # for names that are going away, produce DropSequenceOp
    # directives
    for ddl_name, sch, table in metadata_in_database.difference(metadata_hypertable):
        upgrade_ops.ops.append(
            DropDDLOp(ddl_name, sch, table)
        )
