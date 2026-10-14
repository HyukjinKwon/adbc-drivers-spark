# Copyright (c) 2026 ADBC Drivers Contributors
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

import pytest

from . import spark


def pytest_generate_tests(metafunc) -> None:
    quirks = spark.get_quirks(metafunc.config.getoption("vendor_version"))
    driver_param = f"{quirks.name}:{quirks.short_version}"
    marks = []
    if "thrift" in quirks.short_version:
        marks.append(
            pytest.mark.skip(reason="Thrift lists objects with HiveServer2 calls")
        )
    metafunc.parametrize(
        "driver",
        [pytest.param(driver_param, id=driver_param, marks=marks)],
        scope="module",
        indirect=["driver"],
    )


def test_get_objects_with_temporary_view(driver, conn) -> None:
    # SHOW TABLES lists the session's temporary views in every schema of the
    # session catalog, but DESCRIBE TABLE can't find them there.
    view = f"temp_view_{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cursor:
        cursor.execute(
            f"CREATE TEMPORARY VIEW {driver.quote_one_identifier(view)} AS SELECT 1 AS id"
        )
    try:
        objects = conn.adbc_get_objects(
            depth="all",
            catalog_filter="spark_catalog",
            db_schema_filter=driver.features.current_schema,
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
            cursor.execute(f"DROP VIEW IF EXISTS {driver.quote_one_identifier(view)}")
