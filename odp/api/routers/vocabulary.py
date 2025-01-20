from fastapi import APIRouter, Depends, HTTPException
from jschon import URI
from sqlalchemy import select
from starlette.status import HTTP_404_NOT_FOUND

from odp.api.lib.auth import Authorize
from odp.api.lib.paging import Paginator
from odp.api.models import Page, VocabularyModel
from odp.const import ODPScope
from odp.db import Session
from odp.db.models import Vocabulary
from odp.lib.schema import schema_catalog

router = APIRouter()


def output_vocabulary_model(vocabulary: Vocabulary) -> VocabularyModel:
    return VocabularyModel(
        id=vocabulary.id,
        uri=vocabulary.uri,
        schema_id=vocabulary.schema_id,
        schema_uri=vocabulary.schema.uri,
        schema_=schema_catalog.get_schema(URI(vocabulary.schema.uri)).value,
        static=vocabulary.static,
        keyword_count=len(vocabulary.keywords),
    )


@router.get(
    '/',
    dependencies=[Depends(Authorize(ODPScope.VOCABULARY_READ))],
)
async def list_vocabularies(
        paginator: Paginator = Depends(),
) -> Page[VocabularyModel]:
    """
    List all vocabularies. Requires scope `odp.vocabulary:read`.
    """
    return paginator.paginate(
        select(Vocabulary),
        lambda row: output_vocabulary_model(row.Vocabulary),
    )


@router.get(
    '/{vocabulary_id}',
    dependencies=[Depends(Authorize(ODPScope.VOCABULARY_READ))],
)
async def get_vocabulary(
        vocabulary_id: str,
) -> VocabularyModel:
    """
    Get a vocabulary. Requires scope `odp.vocabulary:read`.
    """
    if not (vocabulary := Session.get(Vocabulary, vocabulary_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    return output_vocabulary_model(vocabulary)
