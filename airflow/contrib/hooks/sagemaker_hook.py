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

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.aws_hook import AwsHook


class SageMakerHook(AwsHook):
    """
    Interact with AWS SageMaker.
    """

    def __init__(self,
                 sagemaker_conn_id='sagemaker_default',
                 job_name=None,
                 use_db_config=False,
                 *args, **kwargs):
        self.sagemaker_conn_id = sagemaker_conn_id
        self.use_db_config = use_db_config
        self.job_name = job_name
        super(SageMakerHook, self).__init__(*args, **kwargs)

    def get_conn(self):
        self.conn = self.get_client_type('sagemaker')
        return self.conn

    def list_training_job(self, job_name):
        sagemaker_conn = self.get_conn()
        return sagemaker_conn.list_training_jobs(NameContains=job_name)

    def list_tuning_job(self, job_name):
        sagemaker_conn = self.get_conn()
        return sagemaker_conn.list_hyper_parameter_tuning_job(NameContains=job_name)

    def create_training_job(self, training_job_config):

        if self.use_db_config:
            if not self.sagemaker_conn_id:
                raise AirflowException("sagemaker connection id must be present to read sagemaker training jobs configuration.")

            sagemaker_conn = self.get_connection(self.sagemaker_conn_id)

            config = sagemaker_conn.extra_dejson.copy()
            training_job_config.update(config)

        # run checks

        return self.get_conn().create_training_job(
            **training_job_config)

    def describe_training_job(self):
        return self.get_conn().describe_training_job(TrainingJobName=self.job_name)

    def create_tunining_job(self, tunning_job_config):

        if self.use_db_config:
            if not self.sagemaker_conn_id:
                raise AirflowException("sagemaker connection id must be present to read sagemaker tunning job configuration.")

            sagemaker_conn = self.get_connection(self.sagemaker_conn_id)

            config = sagemaker_conn.extra_dejson.copy()
            tunning_job_config.update(config)

        return self.get_conn().create_hyper_parameter_tuning_job(
            **tunning_job_config)

