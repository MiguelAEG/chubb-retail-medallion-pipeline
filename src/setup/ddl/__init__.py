"""Módulo DDL: Aprovisionamiento de estructuras de catálogo, esquemas, volúmenes y tablas Delta."""

from src.setup.ddl.init_tables import create_all_tables, get_ddl_statements

__all__ = ["create_all_tables", "get_ddl_statements"]
