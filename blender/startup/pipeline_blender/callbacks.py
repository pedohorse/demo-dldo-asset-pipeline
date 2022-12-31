import bpy
from demo_pipeline import get_director


def update_image_data_uris():
    director = get_director()

    datas = bpy.data.images
    for data in datas.values():
        print(data)
        if 'pipeline' not in data:
            continue
        pipeline_data = data['pipeline']
        print(pipeline_data)
        if 'uri' not in pipeline_data:
            continue
        uri = pipeline_data['uri']
        print(uri)
        if not director.is_uri_dynamic(uri):
            continue
        ass_ver = director.fetch_uri(uri)
        data.filepath = ass_ver.render_sequence_path().format(frame=ass_ver.frame_range()[0])
        print(f'updating "{data}"\'s uri "{uri}" to "{data.filepath}"')


@bpy.app.handlers.persistent
def _on_load_pipeline_check(what, who):
    # first of all we look for places where uris are expected
    #  and update them
    print("he", what, who)
    update_image_data_uris()
