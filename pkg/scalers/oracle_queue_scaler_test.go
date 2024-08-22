package scalers

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

const (
	testOracleQueueProperQueueURL = "https://messaging.mx-queretaro-1.oraclecloud.com"
)

var testOracleQueueEmptyResolvedEnv = map[string]string{}

var testOracleQueueResolvedEnv = map[string]string{
	"QUEUE_URL": testOracleQueueProperQueueURL,
}

var testOracleQueuAuthentication = map[string]string{
	"key": "none",
}

type parseOracleQueueMetadataTestData struct {
	metadata    map[string]string
	authParams  map[string]string
	resolvedEnv map[string]string
	isError     bool
	comment     string
}

var testOracleQueueMetadata = []parseOracleQueueMetadataTestData{
	{map[string]string{},
		testOracleQueuAuthentication,
		testOracleQueueResolvedEnv,
		true,
		"metadata empty"},
}

func TestOracleQueueParseMetadata(t *testing.T) {
	for _, testData := range testOracleQueueMetadata {
		_, err := parseOracleQueueMetadata(&scalersconfig.ScalerConfig{TriggerMetadata: testData.metadata, ResolvedEnv: testData.resolvedEnv, AuthParams: testData.authParams}, logr.Discard())
		if err != nil && !testData.isError {
			t.Errorf("Expected success because %s got error, %s", testData.comment, err)
		}
		if testData.isError && err == nil {
			t.Errorf("Expected error because %s but got success, %#v", testData.comment, testData)
		}
	}
}
