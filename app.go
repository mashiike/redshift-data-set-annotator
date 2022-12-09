package redshiftdatasetannotator

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	_ "github.com/mashiike/redshift-data-sql-driver"
)

var Version string

type App struct {
	cfg Config

	awsAccountID    string
	client          *quicksight.Client
	stsClient       *sts.Client
	dataSrouceCache map[string]*quicksight.DescribeDataSourceOutput

	w io.Writer
}

func New(ctx context.Context, awsAccountID string) (*App, error) {
	cfg, err := loadConfigFile()
	if err != nil {
		return nil, err
	}
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	client := quicksight.NewFromConfig(awsCfg)
	app := &App{
		cfg:             cfg,
		client:          client,
		awsAccountID:    awsAccountID,
		stsClient:       sts.NewFromConfig(awsCfg),
		dataSrouceCache: make(map[string]*quicksight.DescribeDataSourceOutput),
		w:               os.Stdout,
	}
	return app, nil
}

func (app *App) DescribeDataSrouce(ctx context.Context, dataSourceArn string) (*quicksight.DescribeDataSourceOutput, error) {
	if output, ok := app.dataSrouceCache[dataSourceArn]; ok {
		return output, nil
	}
	arnObj, err := arn.Parse(dataSourceArn)
	if err != nil {
		return nil, err
	}
	if arnObj.Service != "quicksight" || !strings.HasPrefix(arnObj.Resource, "datasource/") {
		return nil, fmt.Errorf("%s is not quicksight data source arn", dataSourceArn)
	}
	output, err := app.client.DescribeDataSource(ctx, &quicksight.DescribeDataSourceInput{
		AwsAccountId: aws.String(arnObj.AccountID),
		DataSourceId: aws.String(strings.TrimPrefix(arnObj.Resource, "datasource/")),
	})
	if err != nil {
		return nil, err
	}
	app.dataSrouceCache[dataSourceArn] = output
	return output, nil
}

func (app *App) AWSAccountID() string {
	if app.awsAccountID == "" {
		output, err := app.stsClient.GetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{})
		if err != nil {
			log.Println("[warn] GetCallerIdentity failed:", err)
			return ""
		}
		app.awsAccountID = *output.Account
	}
	return app.awsAccountID
}

func NewUpdateDataSetInput(dataSet *types.DataSet) (*quicksight.UpdateDataSetInput, error) {
	arnObj, err := arn.Parse(*dataSet.Arn)
	if err != nil {
		return nil, err
	}
	input := &quicksight.UpdateDataSetInput{
		AwsAccountId:                       aws.String(arnObj.AccountID),
		DataSetId:                          clonePointer(dataSet.DataSetId),
		ImportMode:                         dataSet.ImportMode,
		Name:                               clonePointer(dataSet.Name),
		PhysicalTableMap:                   cloneMap(dataSet.PhysicalTableMap),
		ColumnGroups:                       cloneSlice(dataSet.ColumnGroups),
		ColumnLevelPermissionRules:         cloneSlice(dataSet.ColumnLevelPermissionRules),
		DataSetUsageConfiguration:          clonePointer(dataSet.DataSetUsageConfiguration),
		FieldFolders:                       cloneMap(dataSet.FieldFolders),
		LogicalTableMap:                    cloneMap(dataSet.LogicalTableMap),
		RowLevelPermissionDataSet:          clonePointer(dataSet.RowLevelPermissionDataSet),
		RowLevelPermissionTagConfiguration: clonePointer(dataSet.RowLevelPermissionTagConfiguration),
	}
	return input, nil
}
