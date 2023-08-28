import re
from typing import Any, Literal, Optional

from pydantic import AnyHttpUrl, BaseModel, Field, root_validator, validator

from odp.const import DOI_REGEX, ID_REGEX, ODPMetadataSchema, SID_REGEX
from odp.const.hydra import GrantType, ResponseType, TokenEndpointAuthMethod
from odp.db.models import AuditCommand, IdentityCommand, TagCardinality


class AccessTokenModel(BaseModel):
    client_id: str
    user_id: Optional[str]
    permissions: dict[str, Literal['*'] | list[str]]


class TagInstanceModel(BaseModel):
    id: str
    tag_id: str
    user_id: Optional[str]
    user_name: Optional[str]
    data: dict[str, Any]
    timestamp: str
    cardinality: TagCardinality
    public: bool


class TagInstanceModelIn(BaseModel):
    tag_id: str
    data: dict[str, Any]


class CatalogModel(BaseModel):
    id: str
    url: str
    record_count: int


class PublishedMetadataModel(BaseModel):
    schema_id: str
    schema_uri: str
    metadata: dict[str, Any]


class PublishedTagInstanceModel(BaseModel):
    tag_id: str
    data: dict[str, Any]
    user_name: Optional[str]
    timestamp: str


class PublishedRecordModel(BaseModel):
    pass


class PublishedSAEONRecordModel(PublishedRecordModel):
    id: str
    doi: Optional[str]
    sid: Optional[str]
    collection_key: str
    collection_name: str
    provider_key: str
    provider_name: str
    metadata_records: list[PublishedMetadataModel]
    tags: list[PublishedTagInstanceModel]
    keywords: Optional[list[str]]
    spatial_north: Optional[float]
    spatial_east: Optional[float]
    spatial_south: Optional[float]
    spatial_west: Optional[float]
    temporal_start: Optional[str]
    temporal_end: Optional[str]
    timestamp: str
    published: bool = Field(True, const=True)
    searchable: Optional[bool]


class PublishedDataCiteRecordModel(PublishedRecordModel):
    doi: str
    url: Optional[AnyHttpUrl]
    metadata: dict[str, Any]


class RetractedRecordModel(BaseModel):
    id: str
    published: bool = Field(False, const=False)


class SearchResult(BaseModel):
    facets: dict[str, list[tuple[str, int]]]  # facet: [(value, count)]
    items: list[PublishedSAEONRecordModel]
    total: int
    page: int
    pages: int


class CatalogRecordModel(BaseModel):
    catalog_id: str
    record_id: str
    published: bool
    published_record: Optional[PublishedSAEONRecordModel | PublishedDataCiteRecordModel]
    reason: str
    timestamp: str
    external_synced: Optional[bool]
    external_error: Optional[str]
    external_error_count: Optional[int]
    index_full_text: Optional[str]
    index_keywords: Optional[list[str]]
    index_facets: Optional[list[dict[str, str]]]
    index_spatial_north: Optional[float]
    index_spatial_east: Optional[float]
    index_spatial_south: Optional[float]
    index_spatial_west: Optional[float]
    index_temporal_start: Optional[str]
    index_temporal_end: Optional[str]
    index_searchable: Optional[bool]


class ClientModel(BaseModel):
    id: str
    name: str
    scope_ids: list[str]
    collection_specific: bool
    collection_keys: dict[str, str]
    grant_types: list[GrantType]
    response_types: list[ResponseType]
    redirect_uris: list[AnyHttpUrl]
    post_logout_redirect_uris: list[AnyHttpUrl]
    token_endpoint_auth_method: TokenEndpointAuthMethod
    allowed_cors_origins: list[AnyHttpUrl]
    client_credentials_grant_access_token_lifespan: Optional[str]


class ClientModelIn(BaseModel):
    id: str = Field(..., regex=ID_REGEX)
    name: str
    secret: str = Field(None, min_length=16)
    scope_ids: list[str]
    collection_specific: bool
    collection_ids: list[str]
    grant_types: list[GrantType]
    response_types: list[ResponseType]
    redirect_uris: list[AnyHttpUrl]
    post_logout_redirect_uris: list[AnyHttpUrl]
    token_endpoint_auth_method: TokenEndpointAuthMethod
    allowed_cors_origins: list[AnyHttpUrl]
    client_credentials_grant_access_token_lifespan: Optional[str] = Field(None, regex='^([0-9]+(ns|us|ms|s|m|h))*$')

    @validator('collection_ids')
    def validate_collection_ids(cls, collection_ids, values):
        try:
            if not values['collection_specific'] and collection_ids:
                raise ValueError("Collections can only be associated with a collection-specific client.")
        except KeyError:
            pass  # ignore: collection_specific validation already failed

        return collection_ids


class CollectionModel(BaseModel):
    id: str
    key: str
    name: str
    doi_key: Optional[str]
    provider_id: str
    provider_key: str
    record_count: int
    tags: list[TagInstanceModel]
    client_ids: list[str]
    role_ids: list[str]
    timestamp: str


class CollectionModelIn(BaseModel):
    key: str = Field(..., regex=ID_REGEX)
    name: str
    doi_key: Optional[str]
    provider_id: str


class ProviderModel(BaseModel):
    id: str
    key: str
    name: str
    collection_keys: dict[str, str]
    timestamp: str


class ProviderModelIn(BaseModel):
    key: str = Field(..., regex=ID_REGEX)
    name: str


class RecordModel(BaseModel):
    id: str
    doi: Optional[str]
    sid: Optional[str]
    collection_id: str
    collection_key: str
    collection_name: str
    provider_id: str
    provider_key: str
    provider_name: str
    schema_id: str
    schema_uri: str
    parent_id: Optional[str]
    parent_doi: Optional[str]
    child_dois: list[str]
    metadata: dict[str, Any]
    validity: dict[str, Any]
    timestamp: str
    tags: list[TagInstanceModel]
    published_catalog_ids: list[str]


class RecordModelIn(BaseModel):
    doi: str = Field(None, regex=DOI_REGEX, title="Digital Object Identifier")
    sid: str = Field(None, regex=SID_REGEX, title="Secondary Identifier")
    collection_id: str
    schema_id: str
    metadata: dict[str, Any]

    @validator('sid', always=True)
    def validate_sid(cls, sid, values):
        try:
            if not values['doi'] and not sid:
                raise ValueError("Secondary ID is mandatory if a DOI is not provided")
        except KeyError:
            pass  # ignore: doi validation already failed

        if sid and re.match(DOI_REGEX, sid):
            raise ValueError("The secondary ID cannot be a DOI")

        return sid

    @validator('schema_id')
    def validate_schema_id(cls, schema_id):
        if schema_id not in (ODPMetadataSchema.SAEON_DATACITE4, ODPMetadataSchema.SAEON_ISO19115):
            raise ValueError("SAEON metadata schema required")

        return schema_id

    @root_validator
    def set_metadata_doi(cls, values):
        """Copy the DOI into the metadata post-validation."""
        try:
            if doi := values['doi']:
                values['metadata']['doi'] = doi
            else:
                values['metadata'].pop('doi', None)
        except KeyError:
            pass  # ignore: doi and/or metadata field validation already failed

        return values


class RoleModel(BaseModel):
    id: str
    scope_ids: list[str]
    collection_specific: bool
    collection_keys: dict[str, str]


class RoleModelIn(BaseModel):
    id: str = Field(..., regex=ID_REGEX)
    scope_ids: list[str]
    collection_specific: bool
    collection_ids: list[str]

    @validator('collection_ids')
    def validate_collection_ids(cls, collection_ids, values):
        try:
            if not values['collection_specific'] and collection_ids:
                raise ValueError("Collections can only be associated with a collection-specific role.")
        except KeyError:
            pass  # ignore: collection_specific validation already failed

        return collection_ids


class SchemaModel(BaseModel):
    id: str
    type: str
    uri: str
    schema_: dict[str, Any]


class ScopeModel(BaseModel):
    id: str
    type: str


class TagModel(BaseModel):
    id: str
    cardinality: TagCardinality
    public: bool
    scope_id: str
    schema_id: str
    schema_uri: str
    schema_: dict[str, Any]
    vocabulary_id: Optional[str]


class UserModel(BaseModel):
    id: str
    email: str
    active: bool
    verified: bool
    name: str
    picture: Optional[str]
    role_ids: list[str]


class UserModelIn(BaseModel):
    id: str
    active: bool
    role_ids: list[str]


class VocabularyTermModel(BaseModel):
    id: str
    data: dict[str, Any]


class VocabularyTermModelIn(BaseModel):
    id: str = Field(..., regex=ID_REGEX)
    data: dict[str, Any]


class VocabularyModel(BaseModel):
    id: str
    scope_id: str
    schema_id: str
    schema_uri: str
    schema_: dict[str, Any]
    static: bool
    terms: list[VocabularyTermModel]


class AuditModel(BaseModel):
    table: str
    tag_id: Optional[str]
    audit_id: int
    client_id: str
    user_id: Optional[str]
    user_name: Optional[str]
    command: AuditCommand
    timestamp: str


class CollectionAuditModel(AuditModel):
    collection_id: str
    collection_key: str
    collection_name: str
    collection_doi_key: Optional[str]
    collection_provider_id: str


class CollectionTagAuditModel(AuditModel):
    collection_tag_id: str
    collection_tag_collection_id: str
    collection_tag_user_id: Optional[str]
    collection_tag_user_name: Optional[str]
    collection_tag_data: dict[str, Any]


class RecordAuditModel(AuditModel):
    record_id: str
    record_doi: Optional[str]
    record_sid: Optional[str]
    record_metadata: dict[str, Any]
    record_collection_id: str
    record_schema_id: str
    record_parent_id: Optional[str]


class RecordTagAuditModel(AuditModel):
    record_tag_id: str
    record_tag_record_id: str
    record_tag_user_id: Optional[str]
    record_tag_user_name: Optional[str]
    record_tag_data: dict[str, Any]


class ProviderAuditModel(AuditModel):
    provider_id: str
    provider_key: str
    provider_name: str


class VocabularyTermAuditModel(AuditModel):
    vocabulary_id: str
    term_id: str
    data: dict[str, Any]


class IdentityAuditModel(BaseModel):
    audit_id: int
    client_id: str
    client_user_id: str | None
    client_user_name: str | None
    command: IdentityCommand
    completed: bool
    error: str | None
    timestamp: str
    user_id: str
    user_email: str
    user_active: bool
    user_roles: list[str]
