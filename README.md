# alembic_extra
extra stuffs that makes working with alembic easier

We can easily add ddl as followed:
-  create_ddl(Alert, HypertableDDL())
-  create_ddl(Alert, HypertableRetentionDDL("3 Months"))
-  create_ddl(Message, HypertableDDL())
-  create_ddl(Message, HypertableRetentionDDL("3 Months"))
