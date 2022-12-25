from demo_pipeline import get_director


def _get_sources_iter(usd_node):
    """
    recurcive method to get the FIRST pathid down the tree
    """
    if usd_node.HasAttribute('pathid'):
        pathid = usd_node.GetAttribute('pathid').Get()
        return {pathid}

    res = set()
    for child in usd_node.GetChildren():
        res.update(_get_sources_iter(child))
    return res


def list_sources(node):
    director = get_director()
    return [director.get_asset_version(pid) for pid in _get_sources_iter(node.stage().GetPrimAtPath('/'))]


def _get_locked_versions_iter(usd_node):
    if usd_node.HasAttribute('pathid') \
            and usd_node.HasAttribute('pathid_resolved_dynamically') \
            and usd_node.HasAttribute('pathid_source_uri'):
        pathid = usd_node.GetAttribute('pathid').Get()
        dynamic = usd_node.GetAttribute('pathid_resolved_dynamically').Get()
        src_uri = usd_node.GetAttribute('pathid_source_uri').Get()
        if dynamic:
            return {pathid}

    res = set()
    for child in usd_node.GetChildren():
        res.update(_get_locked_versions_iter(child))
    return res


def get_locked_versions(node):
    director = get_director()
    return {director.get_asset_version(pathid).asset.path_id: pathid
            for pathid in _get_locked_versions_iter(node.stage().GetPrimAtPath('/'))}


def __old_get_locked_versions(node):
    director = get_director()
    locks = {}
    for child in node.stage().GetPrimAtPath('/').GetChildren():
        if   not child.HasAttribute('pathid') \
          or not child.HasAttribute('pathid_resolved_dynamically') \
          or not child.HasAttribute('pathid_source_uri'):
            continue
        pathid = child.GetAttribute('pathid').Get()
        dynamic = child.GetAttribute('pathid_resolved_dynamically').Get()
        src_uri = child.GetAttribute('pathid_source_uri').Get()
        if src_uri in locks or not dynamic:
            continue
        locks[director.get_asset_version(pathid).asset.path_id] = pathid
    return locks
