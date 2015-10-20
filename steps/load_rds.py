"""
ETL step wrapper to extract data from S3 to RDS
"""
from ..config import Config
from .etl_step import ETLStep
from ..pipeline import CopyActivity
from ..pipeline import MysqlNode
from ..utils.exceptions import ETLInputError

config = Config()
if not hasattr(config, 'mysql'):
    raise ETLInputError('MySQL config not specified in ETL')

MYSQL_CONFIG = config.mysql


class LoadRdsStep(ETLStep):
    """ Load into Rds via MySQL INSERT INTO.

    Args:
        table (str): table to be written to
        column_count (int): Number of columns for the table
        host_name (str): Hostname of the database server. Login details
          will be loaded via config.
        database (str): database name on the RDS host
    """

    def __init__(self, table=None, columns=None, column_count=None,
                 host_name=None, database=None, **kwargs):
        
        if not column_count or column_count <= 0:
            raise ETLInputError('Must have columns to insert data')

        super(LoadRdsStep, self).__init__(**kwargs)

        sql = 'INSERT INTO {0} ({1}) VALUES ({2});'.format(
            table,
            columns,
            ', '.join(['?'] * column_count)
        )
        print sql
        host = MYSQL_CONFIG[host_name]['HOST']
        user = MYSQL_CONFIG[host_name]['USERNAME']
        password = MYSQL_CONFIG[host_name]['PASSWORD']

        self._output = self.create_pipeline_object(
            object_class=MysqlNode,
            schedule=self.schedule,
            host=host,
            database=database,
            table=table,
            username=user,
            password=password,
            sql=sql,
            insert_mode=True
        )
        self.create_pipeline_object(
            object_class=CopyActivity,
            schedule=self.schedule,
            resource=self.resource,
            input_node=self.input,
            output_node=self.output,
            depends_on=self.depends_on,
            max_retries=self.max_retries,
        )

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        step_args = cls.base_arguments_processor(etl, input_args)
        step_args['resource'] = etl.ec2_resource

        return step_args

