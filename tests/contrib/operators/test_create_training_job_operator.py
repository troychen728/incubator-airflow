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

import unittest

from airflow import configuration
from airflow.contrib.hooks.sagemaker_hook import SageMakerHook
from airflow.contrib.operators.sagemaker_create_training_job_operator import SageMakerCreateTrainingJobOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

role = 'arn:aws:iam::123456789:role/service-role/AmazonSageMaker-ExecutionRole-20180608T150937'

bucket = 'test-bucket'

data_key = 'kmeans_lowlevel_example/data'
data_location = 's3://{}/{}'.format(bucket, data_key)

job_name = 'test_job_name'

image = '174872318107.dkr.ecr.us-west-2.amazonaws.com/kmeans:latest'

output_location = 's3://{}/kmeans_example/output'.format(bucket)
create_training_params = \
    {
        "AlgorithmSpecification": {
            "TrainingImage": image,
            "TrainingInputMode": "File"
        },
        "RoleArn": role,
        "OutputDataConfig": {
            "S3OutputPath": output_location
        },
        "ResourceConfig": {
            "InstanceCount": 2,
            "InstanceType": "ml.c4.8xlarge",
            "VolumeSizeInGB": 50
        },
        "TrainingJobName": job_name,
        "HyperParameters": {
            "k": "10",
            "feature_dim": "784",
            "mini_batch_size": "500",
            "force_dense": "True"
        },
        "StoppingCondition": {
            "MaxRuntimeInSeconds": 60 * 60
        },
        "InputDataConfig": [
            {
                "ChannelName": "train",
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": data_location,
                        "S3DataDistributionType": "FullyReplicated"
                    }
                },
                "CompressionType": "None",
                "RecordWrapperType": "None"
            }
        ]
    }


class TestSageMakerTrainingOperator(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.sagemaker_create_training_job_operator.SageMakerCreateTrainingJobOperator')
    def setUp(self, sagemaker_hook_mock):
        configuration.load_test_config()
        self.sagemaker = SageMakerCreateTrainingJobOperator(task_id='test_sagemaker_operator',
                                                            job_name='my_test_job',
                                                            aws_conn_id='aws_default',
                                                            sagemaker_conn_id='sagemaker_test_id',
                                                            training_job_config=create_training_params
                                                            )

    @mock.patch.object(SageMakerHook, 'create_training_job')
    def test_execute_without_failure(self, mock_training):
        mock_training.return_value = {'TrainingJobArn': 'testarn'}
        self.sagemaker.execute(None)
        mock_training.assert_called_once_with(create_training_params)
        self.assertEqual(self.glue.job_name, 'my_test_job')
