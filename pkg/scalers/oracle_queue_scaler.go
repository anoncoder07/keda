/*
Copyright 2021 The KEDA Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scalers

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	auth "github.com/oracle/oci-go-sdk/v65/common/auth"
	ociQueue "github.com/oracle/oci-go-sdk/v65/queue"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/metrics/pkg/apis/external_metrics"

	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
)

const (
	oracleQueueLengthMetricName           = "queueLength"
	oracleDefaultTargetQueueLength        = 5
	activationOracleQueueLengthMetricName = "activationQueueLength"
	oracleQueueMetricType                 = "External"
)

var (
	maxOraclePeekMessages int32 = 32
)

type oracleQueueScaler struct {
	metricType  v2.MetricTargetType
	metadata    *oracleQueueMetadata
	queueClient *ociQueue.QueueClient
	logger      logr.Logger
}

type oracleQueueMetadata struct {
	targetQueueLength           int64
	activationTargetQueueLength int64
	queueId                     string
	queueName                   string
	queueEndPoint               string
	region                      string
	triggerIndex                int
}

// NewOracleQueueScaler creates a new scaler for Oracle queue
func NewOracleQueueScaler(ctx context.Context, config *scalersconfig.ScalerConfig) (Scaler, error) {
	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, fmt.Errorf("error getting scaler metric type: %w", err)
	}

	logger := InitializeLogger(config, "oracle_queue_scaler")

	meta, err := parseOracleQueueMetadata(config, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing azure queue metadata: %w", err)
	}

	queueClient, err := createQueueClient(ctx, meta)
	if err != nil {
		return nil, fmt.Errorf("error creating oracle config provider: %w", err)
	}

	return &oracleQueueScaler{
		metricType:  metricType,
		metadata:    meta,
		queueClient: queueClient,
		logger:      logger,
	}, nil
}

func parseOracleQueueMetadata(config *scalersconfig.ScalerConfig, logger logr.Logger) (*oracleQueueMetadata, error) {
	meta := oracleQueueMetadata{}

	meta.targetQueueLength = oracleDefaultTargetQueueLength
	meta.activationTargetQueueLength = 0

	if queueName, ok := config.TriggerMetadata["queueName"]; ok && queueName != "" {
		meta.queueName = queueName
	} else {
		return nil, fmt.Errorf("no queueName given")
	}

	if queueEndPoint, ok := config.TriggerMetadata["queueEndPoint"]; ok && queueEndPoint != "" {
		meta.queueEndPoint = queueEndPoint
	} else {
		return nil, fmt.Errorf("no queueEndPoint given")
	}

	return &meta, nil
}

func (s *oracleQueueScaler) Close(context.Context) error {
	return nil
}

func (s *oracleQueueScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
	externalMetric := &v2.ExternalMetricSource{
		Metric: v2.MetricIdentifier{
			Name: GenerateMetricNameWithIndex(s.metadata.triggerIndex, kedautil.NormalizeString(fmt.Sprintf("oracle-queue-%s", s.metadata.queueName))),
		},
		Target: GetMetricTarget(s.metricType, s.metadata.targetQueueLength),
	}
	metricSpec := v2.MetricSpec{External: externalMetric, Type: oracleQueueMetricType}
	return []v2.MetricSpec{metricSpec}
}

// GetMetricsAndActivity returns value for a supported metric and an error if there is a problem getting the metric
func (s *oracleQueueScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	queuelen, err := s.getMessageCount(ctx)
	if err != nil {
		s.logger.Error(err, "error getting queue length")
		return []external_metrics.ExternalMetricValue{}, false, err
	}

	metric := GenerateMetricInMili(metricName, float64(queuelen))
	return []external_metrics.ExternalMetricValue{metric}, queuelen > s.metadata.activationTargetQueueLength, nil
}

func (s *oracleQueueScaler) getMessageCount(ctx context.Context) (int64, error) {

	queueMessagesList, err := s.queueClient.GetMessages(ctx, ociQueue.GetMessagesRequest{QueueId: &s.metadata.queueId})
	if err != nil {
		return 0, err
	}
	visibleMessageCount := len(queueMessagesList.Messages)
	return int64(visibleMessageCount), nil

}

func createQueueClient(ctx context.Context, metadata *oracleQueueMetadata) (*ociQueue.QueueClient, error) {

	configProvider, err := auth.OkeWorkloadIdentityConfigurationProvider()
	if err != nil {
		return nil, err
	}

	queueClient, err := ociQueue.NewQueueClientWithConfigurationProvider(configProvider)
	if err != nil {
		return nil, err
	}
	return &queueClient, nil
}
