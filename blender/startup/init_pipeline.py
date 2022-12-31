import bpy
from pipeline_blender.callbacks import _on_load_pipeline_check


def register():
    print('bababa')
    bpy.app.handlers.load_post.append(_on_load_pipeline_check)
