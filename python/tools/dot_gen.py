import sys
import os
import argparse
import subprocess
import tempfile
from demo_pipeline import get_director
from typing import Iterable, Optional


def gen_dot(root_version_uris: Iterable[str] = ()) -> str:
    director = get_director()

    if not root_version_uris:
        root_version_pathids = director.get_data_accessor().get_leaf_asset_version_pathids()
    else:
        root_version_pathids = [director.fetch_uri(uri).path_id for uri in root_version_uris]
    print(root_version_pathids)

    root_versions = [director.get_asset_version(x) for x in root_version_pathids]
    all_versions = set(x.path_id for x in root_versions)
    versions = list(root_versions)
    for version in versions:
        print(version.path_id)
        new_versions = set(x for x in version.get_dependencies() if x.path_id not in all_versions)
        all_versions.update(x.path_id for x in new_versions)
        versions.extend(new_versions)

    dot_labels = [f'{x.path_id.replace("/", "____")} [label = "{x.asset.name} {x.version_id}"];' for x in versions]
    connections = []

    for version in versions:
        connections.extend([f'{x.path_id.replace("/", "____")} -> {version.path_id.replace("/", "____")};' for x in version.get_dependencies()])

    return 'digraph{\n' + \
           '\n'.join(dot_labels) + \
           '\n' + \
           '\n'.join(connections) + \
           '\n}\n'


def main(argv):
    parser = argparse.ArgumentParser(description='generate image of asset dependencies')
    parser.add_argument('uri', nargs='*')

    opts = parser.parse_args(argv[1:])

    dot_code = gen_dot(opts.uri)
    # print(dot_code)

    fd, path = tempfile.mkstemp('.png')
    p = subprocess.Popen(['dot', '-Tpng'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    out, _ = p.communicate(dot_code.encode('UTF-8'))
    if p.poll() != 0:
        raise RuntimeError('error generating dot')
    with open(path, 'wb') as f:
        f.write(out)

    subprocess.Popen(['okular', path]).wait()
    os.close(fd)
    os.unlink(path)


if __name__ == '__main__':
    main(sys.argv)
