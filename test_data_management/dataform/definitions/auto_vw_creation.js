// Copyright 2023 Google LLC

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// https://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const test_objects_str = dataform.projectConfig.vars.test_objects;
const test_objects = eval("[" + test_objects_str + "]");

function create_views(objects_input) {
operate(`create_view_${objects_input[0]}`).queries(`
    DECLARE cols STRING;
    DECLARE filters STRING;

    SET cols = (
        WITH selected_columns as (
        SELECT
        CASE
            WHEN c.data_type = 'STRING' THEN "${dataform.projectConfig.defaultSchema}.dynamic_masking(" || "'" || c.table_name || "'," ||  "'" || c.column_name || "'," || c.column_name || ")" || " AS " || c.column_name
            ELSE c.column_name
        END as column_name
        FROM ${dataform.projectConfig.defaultDatabase}.${dataform.projectConfig.defaultSchema}.INFORMATION_SCHEMA.COLUMNS c
        WHERE table_name = "${objects_input[1]}")
        SELECT STRING_AGG(column_name) AS columns FROM selected_columns
    );

    EXECUTE IMMEDIATE format("""
    CREATE VIEW IF NOT EXISTS ${dataform.projectConfig.vars.testSchema}.${objects_input[0]} AS 
    SELECT %s
    FROM ${dataform.projectConfig.defaultSchema}.${objects_input[1]} src
    WHERE src.region IN (
        SELECT region 
        FROM ${dataform.projectConfig.defaultSchema}.user_metadata
        WHERE user_id = SESSION_USER()
    )
    """,cols)
    ;`
).tags(["data_lake"]).dependencies(["data_masking_fn"])
};

test_objects.forEach(create_views);
