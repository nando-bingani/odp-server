from fastapi import APIRouter, Depends
from sqlalchemy import select

from odp.api.lib.auth import Authorize
from odp.api.lib.paging import Paginator
from odp.api.models import Page, ScopeModel
from odp.const import ODPScope
from odp.db.models import Scope

router = APIRouter()


@router.get(
    '/',
    response_model=Page[ScopeModel],
    dependencies=[Depends(Authorize(ODPScope.SCOPE_READ))],
)
async def list_scopes(
        paginator: Paginator = Depends(),
):
    return paginator.paginate(
        select(Scope),
        lambda row: ScopeModel(
            id=row.Scope.id,
            type=row.Scope.type,
        ),
        sort="array_position(array['openid'], id),"
             "array_position(array['oauth','odp','client'], type::text),"
             "id"
    )
