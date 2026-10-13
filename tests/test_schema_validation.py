from gh_events.stages.schema_validation import compare_schema

EXPECTED = {
    "id": "string",
    "type": "string",
    "actor.id": "bigint",
}


def test_clean_schema_passes():
    actual = {"id": "string", "type": "string", "actor.id": "bigint"}
    missing, wrong_type = compare_schema(actual, EXPECTED)
    assert missing == []
    assert wrong_type == []


def test_missing_field_is_caught():
    actual = {"id": "string", "actor.id": "bigint"}
    missing, wrong_type = compare_schema(actual, EXPECTED)
    assert missing == ["type"]
    assert wrong_type == []


def test_wrong_type_is_caught():
    actual = {"id": "string", "type": "string", "actor.id": "string"}
    missing, wrong_type = compare_schema(actual, EXPECTED)
    assert missing == []
    assert wrong_type == ["actor.id is string, expected bigint"]


def test_extra_fields_are_allowed():
    actual = {
        "id": "string",
        "type": "string",
        "actor.id": "bigint",
        "public": "boolean",
        "payload": "struct<action:string>",
    }
    missing, wrong_type = compare_schema(actual, EXPECTED)
    assert missing == []
    assert wrong_type == []