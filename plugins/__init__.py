from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.GetMovieDetails,
        operators.LoadMovieTable,
        operators.LoadDirectorTable,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
