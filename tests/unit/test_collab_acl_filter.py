"""Unit tests for generic collaboration ACL filtering."""

import pytest
from sqlalchemy.dialects import postgresql

from gulp.api.collab.user import GulpUser
from gulp.api.collab.user_group import GulpUserGroup
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.note import GulpNote
from gulp.api.collab.structs import GulpCollabFilter, GulpUserPermission
from gulp.api.collab_api import GulpCollab
from gulp.plugins.extension.__shared_object import Plugin


GulpCollab.init_mappers()


def _compile_filter(flt: GulpCollabFilter, obj_type=GulpNote) -> str:
    """Compile a collab filter with PostgreSQL syntax for SQL-shape assertions."""
    query = flt.to_select_query(obj_type)
    compiled = query.compile(
        dialect=postgresql.dialect(),
        compile_kwargs={"literal_binds": True},
    )
    return " ".join(str(compiled).split())


def _acl_filter(
    user_ids: list[str] | None,
    group_ids: list[str] | None,
    **kwargs,
) -> GulpCollabFilter:
    """Build a public filter with server-only ACL state attached."""
    flt = GulpCollabFilter(**kwargs)
    flt._set_acl_grants(user_ids, group_ids)
    return flt


def _user(
    user_id: str,
    permission: list[GulpUserPermission],
    groups: list[GulpUserGroup] | None = None,
) -> GulpUser:
    return GulpUser(
        id=user_id,
        type="user",
        user_id=user_id,
        name=user_id,
        time_created=0,
        time_updated=0,
        operation_id=None,
        glyph_id=None,
        description=None,
        tags=[],
        color=None,
        granted_user_ids=[],
        granted_user_group_ids=[],
        pwd_hash="hash",
        permission=permission,
        groups=groups or [],
    )


def _group(group_id: str, permission: list[GulpUserPermission]) -> GulpUserGroup:
    return GulpUserGroup(
        id=group_id,
        type="user_group",
        user_id="admin",
        name=group_id,
        time_created=0,
        time_updated=0,
        operation_id=None,
        glyph_id=None,
        description=None,
        tags=[],
        color=None,
        granted_user_ids=[],
        granted_user_group_ids=[],
        permission=permission,
    )


def _operation(
    user_id: str = "admin",
    granted_user_ids: list[str] | None = None,
    granted_user_group_ids: list[str] | None = None,
) -> GulpOperation:
    return GulpOperation(
        id="admin-only-operation",
        type="operation",
        user_id=user_id,
        name="admin-only-operation",
        time_created=0,
        time_updated=0,
        operation_id=None,
        glyph_id=None,
        description=None,
        tags=[],
        color=None,
        granted_user_ids=granted_user_ids or [],
        granted_user_group_ids=granted_user_group_ids or [],
        index="admin-only-operation",
        operation_data={},
    )


@pytest.mark.unit
def test_acl_filter_public_requires_empty_user_and_group_grants():
    sql = _compile_filter(
        _acl_filter(["ingest"], ["analysts"])
    )

    assert (
        "coalesce(cardinality(note.granted_user_ids), 0) = 0 "
        "AND coalesce(cardinality(note.granted_user_group_ids), 0) = 0"
    ) in sql
    assert "note.user_id IN ('ingest')" in sql
    assert "OR (note.granted_user_ids && ARRAY['ingest'])" in sql
    assert "OR (note.granted_user_group_ids && ARRAY['analysts'])" in sql
    assert "false" not in sql.lower()


@pytest.mark.unit
def test_acl_filter_without_group_membership_keeps_public_and_user_grant_checks():
    sql = _compile_filter(
        _acl_filter(["ingest"], [])
    )

    assert (
        "coalesce(cardinality(note.granted_user_ids), 0) = 0 "
        "AND coalesce(cardinality(note.granted_user_group_ids), 0) = 0"
    ) in sql
    assert "note.user_id IN ('ingest')" in sql
    assert "OR (note.granted_user_ids && ARRAY['ingest'])" in sql
    assert "granted_user_group_ids &&" not in sql


@pytest.mark.unit
def test_acl_filter_for_operations_excludes_admin_only_grants_from_ingest_user():
    sql = _compile_filter(
        _acl_filter(["ingest"], []),
        GulpOperation,
    )

    assert "operation.user_id IN ('ingest')" in sql
    assert "OR (operation.granted_user_ids && ARRAY['ingest'])" in sql
    assert "operation.granted_user_group_ids &&" not in sql
    assert (
        "coalesce(cardinality(operation.granted_user_ids), 0) = 0 "
        "AND coalesce(cardinality(operation.granted_user_group_ids), 0) = 0"
    ) in sql


@pytest.mark.unit
def test_acl_filter_for_plugin_configuration_shared_objects_keeps_acl_and_type_filters():
    sql = _compile_filter(
        _acl_filter(
            ["ingest"],
            [],
            type="shared_object",
            obj_type="plugin_configuration",
        ),
        Plugin.GulpSharedObject,
    )

    assert (
        "coalesce(cardinality(shared_object.granted_user_ids), 0) = 0 "
        "AND coalesce(cardinality(shared_object.granted_user_group_ids), 0) = 0"
    ) in sql
    assert "shared_object.user_id IN ('ingest')" in sql
    assert "OR (shared_object.granted_user_ids && ARRAY['ingest'])" in sql
    assert "shared_object.type ILIKE 'shared_object'" in sql
    assert "shared_object.obj_type ILIKE 'plugin_configuration'" in sql


@pytest.mark.unit
def test_acl_grants_are_server_side_model_extra_not_public_filter_fields():
    flt = _acl_filter(["ingest"], ["analysts"])
    client_supplied = GulpCollabFilter(
        granted_user_ids=["ingest"],
        granted_user_group_ids=["analysts"],
    )
    dumped = flt.model_dump(exclude_none=True)
    client_dumped = client_supplied.model_dump(exclude_none=True)
    schema_props = GulpCollabFilter.model_json_schema().get("properties", {})

    assert dumped["granted_user_ids"] == ["ingest"]
    assert dumped["granted_user_group_ids"] == ["analysts"]
    assert flt.model_extra["granted_user_ids"] == ["ingest"]
    assert flt.model_extra["granted_user_group_ids"] == ["analysts"]
    assert "granted_user_ids" not in client_dumped
    assert "granted_user_group_ids" not in client_dumped
    assert "granted_user_ids" not in schema_props
    assert "granted_user_group_ids" not in schema_props


@pytest.mark.unit
async def test_restrict_filter_to_multi_permission_user_keeps_acl_grants(monkeypatch):
    group = _group("analysts", [GulpUserPermission.READ])
    user = _user(
        "ingest",
        [GulpUserPermission.READ, GulpUserPermission.INGEST],
        groups=[group],
    )

    async def _get_by_id(_sess, user_id):
        assert user_id == "ingest"
        return user

    monkeypatch.setattr(GulpUser, "get_by_id", _get_by_id)

    flt = await GulpOperation._restrict_flt_to_user(
        sess=object(),
        user_id="ingest",
        flt=GulpCollabFilter(),
    )
    sql = _compile_filter(flt, GulpOperation)

    assert flt._get_acl_grants() == (["ingest"], ["analysts"])
    assert "operation.user_id IN ('ingest')" in sql
    assert "OR (operation.granted_user_ids && ARRAY['ingest'])" in sql
    assert "OR (operation.granted_user_group_ids && ARRAY['analysts'])" in sql
    assert (
        "coalesce(cardinality(operation.granted_user_ids), 0) = 0 "
        "AND coalesce(cardinality(operation.granted_user_group_ids), 0) = 0"
    ) in sql


@pytest.mark.unit
@pytest.mark.parametrize(
    "permission",
    [
        [GulpUserPermission.READ],
        [GulpUserPermission.READ, GulpUserPermission.INGEST],
        [GulpUserPermission.READ, GulpUserPermission.EDIT],
        [GulpUserPermission.READ, GulpUserPermission.DELETE],
        [
            GulpUserPermission.READ,
            GulpUserPermission.INGEST,
            GulpUserPermission.EDIT,
        ],
    ],
)
def test_non_admin_permission_sets_do_not_bypass_admin_only_operation(permission):
    user = _user("ingest", permission)
    operation = _operation(granted_user_ids=["admin"])

    assert user.is_admin() is False
    assert user.has_permission([GulpUserPermission.ADMIN]) is False
    assert user.check_object_access(operation) is False


@pytest.mark.unit
def test_has_permission_requires_requested_permission_not_permission_count():
    read_ingest = _user(
        "ingest",
        [GulpUserPermission.READ, GulpUserPermission.INGEST],
    )
    read_edit_delete = _user(
        "editor",
        [
            GulpUserPermission.READ,
            GulpUserPermission.EDIT,
            GulpUserPermission.DELETE,
        ],
    )

    assert read_ingest.has_permission([GulpUserPermission.READ]) is True
    assert read_ingest.has_permission([GulpUserPermission.INGEST]) is True
    assert read_ingest.has_permission([GulpUserPermission.ADMIN]) is False
    assert read_ingest.is_admin() is False
    assert read_edit_delete.has_permission([GulpUserPermission.ADMIN]) is False
    assert read_edit_delete.is_admin() is False


@pytest.mark.unit
def test_admin_owner_user_grant_and_group_grant_can_access_operation():
    admin = _user("admin", [GulpUserPermission.ADMIN])
    owner = _user("ingest", [GulpUserPermission.READ])
    user_granted = _user(
        "ingest",
        [GulpUserPermission.READ, GulpUserPermission.INGEST],
    )
    group_granted = _user(
        "ingest",
        [GulpUserPermission.READ],
        groups=[_group("analysts", [GulpUserPermission.READ])],
    )

    assert admin.check_object_access(_operation(granted_user_ids=["admin"])) is True
    assert owner.check_object_access(_operation(user_id="ingest")) is True
    assert user_granted.check_object_access(
        _operation(granted_user_ids=["ingest"])
    ) is True
    assert group_granted.check_object_access(
        _operation(granted_user_group_ids=["analysts"])
    ) is True


@pytest.mark.unit
def test_empty_grants_operation_keeps_existing_public_semantics():
    user = _user("ingest", [GulpUserPermission.READ, GulpUserPermission.INGEST])
    operation = _operation(granted_user_ids=[], granted_user_group_ids=[])

    assert user.check_object_access(operation) is True
