from sigma.pipelines.elasticsearch.windows import ecs_windows
from sigma.processing.pipeline import ProcessingPipeline

from gulp.defs import GulpPluginType
from gulp.plugin import PluginBase
from gulp.plugin_internal import GulpPluginParams
from gulp.utils import logger


class Plugin(PluginBase):
    def desc(self) -> str:
        return "provides pysigma pipeline for converting Windows specific sigma rules to elasticsearch DSL queries targeting Windows events stored by Gulp."

    def type(self) -> GulpPluginType:
        return GulpPluginType.SIGMA

    def name(self) -> str:
        return "sigma_pipeline_windows_ecs"

    def version(self) -> str:
        return "1.0"

    async def pipeline(
        self, plugin_params: GulpPluginParams = None, **kwargs
    ) -> ProcessingPipeline:
        logger().debug("params: %s" % (plugin_params))
        pipeline = await self.sigma_plugin_initialize(
            pipeline=ecs_windows(),
            mapping_file="windows.json",
            product="windows",
            plugin_params=plugin_params,
        )
        return pipeline
