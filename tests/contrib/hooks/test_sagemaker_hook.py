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
#


import json
import unittest

from airflow import configuration
from airflow import models
from airflow.utils import db

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

role = 'arn:aws:iam::123456789:role/service-role/AmazonSageMaker-ExecutionRole-20180608T150937'

bucket='test-bucket'


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



class TestSageMakerHook(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        db.merge_conn(
            models.Connection(
                conn_id='sagemaker_test',
                conn_type='sagemaker',
                login='access_id',
                password='access_key',
                extra=json.dumps({"ResourceConfig": {
                                  "InstanceCount": 2,
                                  "InstanceType": "ml.c4.8xlarge",
                                  "VolumeSizeInGB": 50
                                  }})
            )
        )

    @mock.patch('airflow.contrib.hooks.sagemaker_hook.SageMakerHook.get_client_type', autospec=True)
    def test_conn(self, mock_get_client):
        from airflow.contrib.hooks.sagemaker_hook import SageMakerHook
        hook = SageMakerHook(sagemaker_conn_id='sagemaker_test')
        self.assertEqual(hook.sagemaker_conn_id, 'sagemaker_test')
        hook.get_conn()
        mock_get_client.assert_called_once_with(hook, client_type='sagemaker')

    @mock.patch('airflow.contrib.hooks.sagemaker_hook.SageMakerHook.get_conn', autospec=True)
    def test_create_training_job(self, mock_client):
        from airflow.contrib.hooks.sagemaker_hook import SageMakerHook
        hook = SageMakerHook(sagemaker_conn_id='sagemaker_test')
        mock_session = mock.Mock()
        attrs = {'create_training_job.return_value': 'outputarn'}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook.create_training_job(create_training_params)
        mock_session.create_training_job.assert_called_once_with(**create_training_params)

if __name__ == '__main__':
    unittest.main()
