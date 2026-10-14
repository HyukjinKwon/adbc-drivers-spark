# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import uuid

import adbc_drivers_validation.tests.connection

from . import spark


def pytest_generate_tests(metafunc) -> None:
    quirks = [spark.get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.connection.generate_tests(quirks, metafunc)


class TestConnection(adbc_drivers_validation.tests.connection.TestConnection):
    def test_get_objects_temporary_view(self, driver, conn) -> None:
        # SHOW TABLES lists temporary views in every schema of the session
        # catalog, but DESCRIBE TABLE can't find them there. Other catalogs,
        # such as Iceberg, don't list them, so look in spark_catalog.default.
        view = f"temp_view_{uuid.uuid4().hex[:8]}"
        with conn.cursor() as cursor:
            cursor.execute(
                f"CREATE TEMPORARY VIEW {driver.quote_one_identifier(view)} AS SELECT 1 AS id"
            )
        try:
            objects = conn.adbc_get_objects(
                depth="all",
                catalog_filter="spark_catalog",
                db_schema_filter="default",
                table_name_filter=view,
            )
            tables = [
                t["table_name"]
                for obj in objects.read_all().to_pylist()
                for s in obj["catalog_db_schemas"]
                for t in s["db_schema_tables"]
            ]
            assert tables == [view]
        finally:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"DROP VIEW IF EXISTS {driver.quote_one_identifier(view)}"
                )
