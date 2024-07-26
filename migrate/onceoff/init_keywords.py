from odp.const.db import KeywordStatus, SchemaType
from odp.db import Session
from odp.db.models import Keyword


def institutions():
    def add(title, abbr, ror=None):
        if not Session.get(Keyword, title):
            Keyword(
                id=f'Institution:{title}',
                parent_id='Institution',
                status=KeywordStatus.approved,
                data=dict(
                    title=title,
                    abbr=abbr,
                    ror=ror,
                )).save()

    if not Session.get(Keyword, 'Institution'):
        Keyword(
            id='Institution',
            data={},
            status=KeywordStatus.approved,
            child_schema_id='Keyword.Institution',
            child_schema_type=SchemaType.keyword
        ).save()

    add('South African Environmental Observation Network', 'SAEON', 'https://ror.org/041j42q70')
    add('Department of Forestry, Fisheries and the Environment', 'DFFE', 'https://ror.org/01vm69c87')
    add('University of Cape Town', 'UCT', 'https://ror.org/03p74gp79')
    add('Department of Agriculture, Fisheries and Forestry', 'DAFF')
    add('South African Institute for Aquatic Biodiversity', 'SAIAB', 'https://ror.org/00bfgxv06')
    add('South African Weather Service', 'SAWS', 'https://ror.org/03mef6610')
    add('Department of Science and Innovation', 'DSI', 'https://ror.org/048kh7197')
    add('Laboratoire de Météorologie Dynamique', 'LMD', 'https://ror.org/000ehr937')
    add('Atlantic Oceanographic and Meteorological Laboratory', 'AOML', 'https://ror.org/042r9xb17')

    Session.commit()
