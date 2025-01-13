from datetime import datetime, timezone

from fastapi import HTTPException
from jschon import JSON
from sqlalchemy import select
from starlette.status import HTTP_403_FORBIDDEN, HTTP_404_NOT_FOUND, HTTP_422_UNPROCESSABLE_ENTITY

from odp.api.lib.auth import Authorized
from odp.api.lib.schema import get_tag_schema
from odp.api.models import TagInstanceModel, TagInstanceModelIn
from odp.const.db import AuditCommand, TagCardinality, TagType
from odp.db import Session
from odp.db.models import (
    Collection,
    CollectionTag,
    CollectionTagAudit,
    Keyword,
    Package,
    PackageTag,
    PackageTagAudit,
    Record,
    RecordTag,
    RecordTagAudit,
    Tag,
)

Taggable = Collection | Package | Record
TagInstance = CollectionTag | PackageTag | RecordTag


class Tagger:
    _tag_instance_classes = {
        TagType.collection: CollectionTag,
        TagType.package: PackageTag,
        TagType.record: RecordTag,
    }

    _tag_audit_classes = {
        TagType.collection: CollectionTagAudit,
        TagType.package: PackageTagAudit,
        TagType.record: RecordTagAudit,
    }

    def __init__(self, tag_type: TagType):
        self.tag_type = tag_type
        self.tag_instance_cls = self._tag_instance_classes[tag_type]
        self.tag_audit_cls = self._tag_audit_classes[tag_type]
        self.obj_id_col = f'{tag_type}_id'

    async def set_tag_instance(
            self,
            tag_instance_in: TagInstanceModelIn,
            obj: Taggable,
            auth: Authorized,
    ) -> TagInstance | None:
        """Create or update a tag instance attached to `obj`.

        Return the created/updated instance, or None if no change was made.
        """
        if not (tag := Session.get(Tag, (tag_instance_in.tag_id, self.tag_type))):
            raise HTTPException(HTTP_404_NOT_FOUND)

        if tag.vocabulary_id is not None:
            if not (keyword := Session.execute(
                    select(Keyword)
                            .where(Keyword.vocabulary_id == tag.vocabulary_id)
                            .where(Keyword.key == tag_instance_in.keyword)
            ).scalar_one_or_none()):
                raise HTTPException(HTTP_404_NOT_FOUND, 'Keyword not found')

            tag_instance_in_keyword_id = keyword.id

        elif tag_instance_in.keyword is not None:
            raise HTTPException(HTTP_422_UNPROCESSABLE_ENTITY, 'Keyword not allowed')

        else:
            tag_instance_in_keyword_id = None

        # only one tag instance per object is allowed
        # update existing tag instance if found
        if tag.cardinality == TagCardinality.one:
            if tag_instance := Session.execute(
                    select(self.tag_instance_cls)
                            .where(getattr(self.tag_instance_cls, self.obj_id_col) == obj.id)
                            .where(self.tag_instance_cls.tag_id == tag_instance_in.tag_id)
            ).scalar_one_or_none():
                command = AuditCommand.update
            else:
                command = AuditCommand.insert

        # one tag instance per user per object is allowed
        # update a user's existing tag instance if found
        elif tag.cardinality == TagCardinality.user:
            if tag_instance := Session.execute(
                    select(self.tag_instance_cls)
                            .where(getattr(self.tag_instance_cls, self.obj_id_col) == obj.id)
                            .where(self.tag_instance_cls.tag_id == tag_instance_in.tag_id)
                            .where(self.tag_instance_cls.user_id == auth.user_id)
            ).scalar_one_or_none():
                command = AuditCommand.update
            else:
                command = AuditCommand.insert

        # multiple tag instances are allowed per user per object
        # can only insert/delete
        elif tag.cardinality == TagCardinality.multi:
            command = AuditCommand.insert

        else:
            assert False

        if command == AuditCommand.insert:
            tag_instance_kwargs = {self.obj_id_col: obj.id} | dict(
                tag_id=tag_instance_in.tag_id,
                tag_type=self.tag_type,
            )
            tag_instance = self.tag_instance_cls(**tag_instance_kwargs)

        if (
                tag_instance.data != tag_instance_in.data or
                tag_instance.keyword_id != tag_instance_in_keyword_id
        ):
            tag_schema = await get_tag_schema(tag_instance_in)
            validity = tag_schema.evaluate(JSON(tag_instance_in.data)).output('detailed')
            if not validity['valid']:
                raise HTTPException(HTTP_422_UNPROCESSABLE_ENTITY, validity)

            tag_instance.user_id = auth.user_id
            tag_instance.vocabulary_id = tag.vocabulary_id
            tag_instance.keyword_id = tag_instance_in_keyword_id
            tag_instance.data = tag_instance_in.data
            tag_instance.timestamp = (timestamp := datetime.now(timezone.utc))
            tag_instance.save()

            obj.timestamp = timestamp
            obj.save()

            self._create_audit_record(tag_instance, command, auth, timestamp)

            return tag_instance

    async def delete_tag_instance(
            self,
            tag_instance_id: str,
            obj: Taggable,
            auth: Authorized,
    ) -> None:
        if not (tag_instance := Session.execute(
                select(self.tag_instance_cls)
                        .where(self.tag_instance_cls.id == tag_instance_id)
                        .where(getattr(self.tag_instance_cls, self.obj_id_col) == obj.id)
        ).scalar_one_or_none()):
            raise HTTPException(HTTP_404_NOT_FOUND)

        # only admin users can delete other users' tags
        if not auth.scope.is_admin and tag_instance.user_id != auth.user_id:
            raise HTTPException(HTTP_403_FORBIDDEN)

        tag_instance.delete()

        obj.timestamp = (timestamp := datetime.now(timezone.utc))
        obj.save()

        self._create_audit_record(tag_instance, AuditCommand.delete, auth, timestamp)

    def _create_audit_record(
            self,
            tag_instance: TagInstance,
            command: AuditCommand,
            auth: Authorized,
            timestamp: datetime,
    ) -> None:
        tag_audit_kwargs = {f'_{self.obj_id_col}': getattr(tag_instance, self.obj_id_col)} | dict(
            client_id=auth.client_id,
            user_id=auth.user_id,
            command=command,
            timestamp=timestamp,
            _id=tag_instance.id,
            _tag_id=tag_instance.tag_id,
            _user_id=tag_instance.user_id,
            _data=tag_instance.data,
            _keyword_id=tag_instance.keyword_id,
        )
        self.tag_audit_cls(**tag_audit_kwargs).save()


def output_tag_instance_model(tag_instance: Taggable) -> TagInstanceModel:
    tag_instance_args = dict(
        id=tag_instance.id,
        tag_id=tag_instance.tag_id,
        user_id=tag_instance.user_id,
        user_name=tag_instance.user.name if tag_instance.user_id else None,
        user_email=tag_instance.user.email if tag_instance.user_id else None,
        data=tag_instance.data,
        timestamp=tag_instance.timestamp.isoformat(),
        cardinality=tag_instance.tag.cardinality,
        public=tag_instance.tag.public,
        vocabulary_id=tag_instance.vocabulary_id,
    )
    if tag_instance.vocabulary_id:
        kw = tag_instance.keyword
        kw_ids = [kw.id]
        kw_keys = [kw.key]
        while kw.parent_id is not None:
            kw = kw.parent
            kw_ids.insert(0, kw.id)
            kw_keys.insert(0, kw.key)
        tag_instance_args |= dict(
            keyword_ids=kw_ids,
            keyword_keys=kw_keys,
        )

    return TagInstanceModel(**tag_instance_args)
