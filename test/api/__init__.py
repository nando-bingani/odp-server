from odp.const import ODPScope

all_scopes = [s for s in ODPScope]


def all_scopes_excluding(scope):
    return [s for s in ODPScope if s != scope]
