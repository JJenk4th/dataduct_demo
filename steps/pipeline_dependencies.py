"""
ETL step for pipeline dependencies using transform step
"""
import os

from .transform import TransformStep
from ..utils import constants as const
from ..config import Config

config = Config()
NAME_PREFIX = config.etl.get('NAME_PREFIX', '')
DEPENDENCY_OVERRIDE = config.etl.get('DEPENDENCY_OVERRIDE', False)
SNS_TOPIC_ARN = config.etl.get('SNS_TOPIC_ARN_FAILURE', None)


class PipelineDependenciesStep(TransformStep):
    """PipelineDependencies Step class that helps wait for other pipelines
        to finish
    """

    def __init__(self,
                 id,
                 pipeline_name,
                 dependent_pipelines=None,
                 dependent_pipelines_ok_to_fail=None,
                 refresh_rate=300,
                 start_date=None,
                 script_arguments=None,
                 **kwargs):
        """Constructor for the QATransformStep class

        Args:
            sns_arn(str): sns topic arn for QA steps
            script_arguments(list of str): list of arguments to the script
            **kwargs(optional): Keyword arguments directly passed to base class
        """

        if script_arguments is None:
            script_arguments = list()

        if dependent_pipelines is None and dependent_pipelines_ok_to_fail is None:
            raise ValueError('Must have some dependencies for dependency step')

        if DEPENDENCY_OVERRIDE:
            command = 'ls'
            script = None
            script_arguments = None
        else:
            command = None
            if start_date is None:
                start_date = "#{format(@scheduledStartTime,'YYYY-MM-dd')}"

            script_arguments.extend(
                [
                    '--pipeline_name=%s' % pipeline_name,
                    '--start_date=%s' % start_date,
                    '--refresh_rate=%s' % str(refresh_rate),
                    '--sns_topic_arn=%s' % SNS_TOPIC_ARN,
                ]
            )

            if dependent_pipelines:
                script_arguments.append('--dependencies')
                script_arguments.extend([
                    pipeline if not NAME_PREFIX else NAME_PREFIX + '_' + pipeline
                    for pipeline in dependent_pipelines
                ])

            if dependent_pipelines_ok_to_fail:
                script_arguments.append('--dependencies_ok_to_fail')
                script_arguments.extend([
                    pipeline if not NAME_PREFIX else NAME_PREFIX + '_' + pipeline
                    for pipeline in dependent_pipelines_ok_to_fail
                ])

            steps_path = os.path.abspath(os.path.dirname(__file__))
            script = os.path.join(steps_path, const.DEPENDENCY_SCRIPT_PATH)

        super(PipelineDependenciesStep, self).__init__(
            id=id,
            script=script,
            command=command,
            script_arguments=script_arguments,
            no_output=True,
            **kwargs)

        self._output = None

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        input_args = cls.pop_inputs(input_args)
        step_args = cls.base_arguments_processor(etl, input_args)
        step_args['resource'] = etl.ec2_resource
        step_args['pipeline_name'] = etl.name

        return step_args
