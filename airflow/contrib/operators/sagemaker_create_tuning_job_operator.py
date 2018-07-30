# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from airflow.contrib.hooks.sagemaker_hook import SageMakerHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowException


class SageMakerCreateHyperParameterTuningJobOperator(BaseOperator):

    """
       Initiate a SageMaker HyperParameter Tuning Job

       This operator returns The ARN of the model created in Amazon SageMaker

       :param region_name: The AWS region_name
       :type region_name: string
       :param tuning_job_config:
       The configuration necessary to start a tuning job (templated)
       :type tuning_job_config: dict
       :param sagemaker_conn_id: The SageMaker connection ID to use.
       :type sagemaker_conn_id: string
       :param use_db_config: Whether or not to use db config
       associated with sagemaker_conn_id.
       If set to true, will automatically update the training config
       with what's in db, so the db config doesn't need to
       included everything, but what's there does replace the ones
       in the training_job_config, so be careful
       :type use_db_config: bool
       :param aws_conn_id: The AWS connection ID to use.
       :type aws_conn_id: string

       **Example**:
           The following operator would start a training job when executed

            sagemaker_tuning =
               SageMakerCreateHyperParameterTuningJobOperator(
                   task_id='sagemaker_tuning',
                   training_job_config=config,
                   job_name='my_sagemaker_tuning'
                   sagemaker_conn_id='sagemaker_customers_conn'
                   aws_conn_id='aws_customers_conn'
               )
       """

    template_fields = ['tunning_job_config']
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self,
                 sagemaker_conn_id=None,
                 region_name=None,
                 tuning_job_config=None,
                 use_db_config=False,
                 *args, **kwargs):
        super(SageMakerCreateHyperParameterTuningJobOperator, self)\
            .__init__(*args, **kwargs)

        self.sagemaker_conn_id = sagemaker_conn_id
        self.region_name = region_name
        self.tuning_job_config = tuning_job_config
        self.use_db_config = use_db_config

    def execute(self, context):
        sagemaker = SageMakerHook(sagemaker_conn_id=self.sagemaker_conn_id,
                                  region_name=self.region_name,
                                  use_db_config=self.use_db_config)

        self.log.info(
            "Creating SageMaker Hyper Parameter Tunning Job %s"
            % self.tuning_job_config['HyperParameterTuningJobName']
        )

        response = sagemaker.create_tuning_job(
            self.tuning_job_config
        )
        if not response['ResponseMetadata']['HTTPStatusCode'] \
           == 200:
            raise AirflowException(
                "Sagemaker Training Job creation failed: %s" % response)
        else:
            return response
