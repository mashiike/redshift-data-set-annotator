package redshiftdatasetannotator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	_ "github.com/mashiike/redshift-data-sql-driver"
	"github.com/samber/lo"
)

type AnnotateOption struct {
	DataSetID              string `help:"task ID" required:""`
	DryRun                 bool   `help:"if true, no update data set and display plan"`
	ForceRename            bool   `help:"The default is to keep any renaming that has already taken place. Enabling this option forces a name overwrite."`
	ForceUpdateDescription bool   `help:"The default is to keep any renaming that has already taken place. Enabling this option forces a description overwrite."`
	Verbose                bool   `help:"Outputs the input information for the UpdateDataSet API"`
}

func (app *App) RunAnnotate(ctx context.Context, opt *AnnotateOption) error {
	if opt.DryRun {
		log.Println("[info] ************* start dry run ****************")
	}
	describeDataSetOutput, err := app.client.DescribeDataSet(ctx, &quicksight.DescribeDataSetInput{
		AwsAccountId: aws.String(app.AWSAccountID()),
		DataSetId:    aws.String(opt.DataSetID),
	})
	if err != nil {
		return fmt.Errorf("DescribeDataSet:%w", err)
	}
	if describeDataSetOutput.Status != http.StatusOK {
		return fmt.Errorf("unexpected data set status:%d", describeDataSetOutput.Status)
	}
	log.Printf("[debug] data set `%s` name=`%s`", *describeDataSetOutput.DataSet.Arn, *describeDataSetOutput.DataSet.Name)
	updateDataSetInput, err := NewUpdateDataSetInput(describeDataSetOutput.DataSet)
	if err != nil {
		return fmt.Errorf("NewUpdateDataSetInput: %w", err)
	}
	var needUpdate bool
	for physicalTableID, physicalTable := range updateDataSetInput.PhysicalTableMap {
		log.Printf("[debug] found physical table `%s` in `%s`", physicalTableID, *updateDataSetInput.Name)
		relationalTable, ok := physicalTable.(*types.PhysicalTableMemberRelationalTable)
		if !ok {
			log.Printf("[debug] physical table `%s` is not relational table", physicalTableID)
			continue
		}
		describeDataSourceOutput, err := app.DescribeDataSrouce(ctx, *relationalTable.Value.DataSourceArn)
		if err != nil {
			return fmt.Errorf("physical table `%s`: %w", physicalTableID, err)
		}
		if describeDataSourceOutput.DataSource.Type != types.DataSourceTypeRedshift {
			log.Printf("[debug] physical table `%s` data source type is not redshift. type is `%s`", physicalTableID, describeDataSourceOutput.DataSource.Type)
			continue
		}
		log.Printf("[debug] physical table `%s` data source `\"%s\".\"%s\"` in `%s`", physicalTableID, *relationalTable.Value.Schema, *relationalTable.Value.Name, *relationalTable.Value.DataSourceArn)
		columnAnnotations, err := app.GetColumnAnnotations(ctx, describeDataSourceOutput.DataSource, relationalTable.Value)
		if err != nil {
			return fmt.Errorf("GetColumnAnnotations: %w", err)
		}

		for logicalTableID, logicalTable := range updateDataSetInput.LogicalTableMap {
			if *logicalTable.Source.PhysicalTableId != physicalTableID {
				continue
			}
			log.Printf("[debug] logical table `%s` source physical table `%s`", logicalTableID, physicalTableID)
			renameColumnOperations := make(map[string]*types.TransformOperationMemberRenameColumnOperation)
			castColumnOperations := make(map[string]*types.TransformOperationMemberCastColumnTypeOperation)
			tagColumnOperations := make(map[string]*types.TransformOperationMemberTagColumnOperation)
			untagColumnOperations := make(map[string]*types.TransformOperationMemberUntagColumnOperation)
			filterColumnOperations := make([]*types.TransformOperationMemberFilterOperation, 0)
			createColumnOperations := make([]*types.TransformOperationMemberCreateColumnsOperation, 0)
			projectOperations := make([]*types.TransformOperationMemberProjectOperation, 0)

			otherOperations := make([]types.TransformOperation, 0, len(logicalTable.DataTransforms))
			for _, dataTransform := range logicalTable.DataTransforms {
				switch t := dataTransform.(type) {
				case *types.TransformOperationMemberRenameColumnOperation:
					renameColumnOperations[*t.Value.ColumnName] = t
				case *types.TransformOperationMemberCastColumnTypeOperation:
					castColumnOperations[*t.Value.ColumnName] = t
				case *types.TransformOperationMemberTagColumnOperation:
					tagColumnOperations[*t.Value.ColumnName] = t
				case *types.TransformOperationMemberUntagColumnOperation:
					untagColumnOperations[*t.Value.ColumnName] = t
				case *types.TransformOperationMemberFilterOperation:
					filterColumnOperations = append(filterColumnOperations, t)
				case *types.TransformOperationMemberCreateColumnsOperation:
					createColumnOperations = append(createColumnOperations, t)
				case *types.TransformOperationMemberProjectOperation:
					projectOperations = append(projectOperations, t)
				default:
					log.Printf("[warn] unknown transform operation %T, keep same.", t)
					otherOperations = append(otherOperations, t)
				}
			}
			for _, physicalColumn := range relationalTable.Value.InputColumns {
				physicalColumnName := *physicalColumn.Name
				columnAnnotation, ok := columnAnnotations[physicalColumnName]
				if !ok {
					log.Printf("[debug] skip column `%s` in logical table `%s`", physicalColumnName, logicalTableID)
					continue
				}

				// check rename
				var logicalColumnName string
				oldColumnName := physicalColumnName
				if columnAnnotation.Name != nil {
					renameColumnOperation, ok := renameColumnOperations[physicalColumnName]
					if !ok {
						logicalColumnName = *columnAnnotation.Name
						renameColumnOperation = &types.TransformOperationMemberRenameColumnOperation{
							Value: types.RenameColumnOperation{
								ColumnName:    aws.String(physicalColumnName),
								NewColumnName: aws.String(logicalColumnName),
							},
						}
						log.Printf("[debug] new rename column operation `%s` to `%s` in logical table `%s`", physicalColumnName, logicalColumnName, logicalTableID)
						log.Printf("[info] rename field `%s` to `%s`", physicalColumnName, logicalColumnName)
						needUpdate = true
					} else {
						if *renameColumnOperation.Value.NewColumnName != *columnAnnotation.Name && opt.ForceRename {
							oldColumnName = *renameColumnOperation.Value.NewColumnName
							logicalColumnName = *columnAnnotation.Name
							renameColumnOperation.Value.NewColumnName = aws.String(logicalColumnName)
							log.Printf("[debug] rewrite rename column operation `%s` to `%s` in logical table `%s`", physicalColumnName, logicalColumnName, logicalTableID)
							log.Printf("[info] rename field for `%s`: rewrite `%s` to `%s`", physicalColumnName, oldColumnName, logicalColumnName)
							needUpdate = true
						} else {
							logicalColumnName = *renameColumnOperation.Value.NewColumnName
							log.Printf("[debug] keep rename column operation `%s` to `%s` in logical table `%s`", physicalColumnName, logicalColumnName, logicalTableID)
						}
					}
					renameColumnOperations[physicalColumnName] = renameColumnOperation
				} else {
					logicalColumnName = physicalColumnName
				}

				// Check the impact of rename
				if oldColumnName != logicalColumnName {
					//check cast operation
					if op, ok := castColumnOperations[oldColumnName]; ok {
						op.Value.ColumnName = aws.String(logicalColumnName)
						castColumnOperations[logicalColumnName] = op
						delete(castColumnOperations, oldColumnName)
						log.Printf("[debug] swap cast operation `%s` to `%s` in logical table `%s`", oldColumnName, logicalColumnName, logicalTableID)
						needUpdate = true
					}

					//check tag operation
					if op, ok := tagColumnOperations[oldColumnName]; ok {
						op.Value.ColumnName = aws.String(logicalColumnName)
						tagColumnOperations[logicalColumnName] = op
						delete(tagColumnOperations, oldColumnName)
						log.Printf("[debug] swap tag operation `%s` to `%s` in logical table `%s`", oldColumnName, logicalColumnName, logicalTableID)
						needUpdate = true
					}

					//check untag operation
					if op, ok := untagColumnOperations[oldColumnName]; ok {
						op.Value.ColumnName = aws.String(logicalColumnName)
						untagColumnOperations[logicalColumnName] = op
						delete(tagColumnOperations, oldColumnName)
						log.Printf("[debug] swap untag operation `%s` to `%s` in logical table `%s`", oldColumnName, logicalColumnName, logicalTableID)
						needUpdate = true
					}

					//check create column operation
					for i, op := range createColumnOperations {
						for j, column := range op.Value.Columns {
							if strings.Contains(*column.Expression, oldColumnName) {
								column.Expression = aws.String(strings.ReplaceAll(*column.Expression, oldColumnName, logicalColumnName))
								log.Printf("[debug] rewrite create column operation `%s` expression=`%s` in logical table `%s`", *column.ColumnName, *column.Expression, logicalTableID)
								needUpdate = true
							}
							op.Value.Columns[j] = column
						}
						createColumnOperations[i] = op
					}

					//check filter column operation
					for i, op := range filterColumnOperations {
						if strings.Contains(*op.Value.ConditionExpression, oldColumnName) {
							op.Value.ConditionExpression = aws.String(strings.ReplaceAll(*op.Value.ConditionExpression, oldColumnName, logicalColumnName))
							log.Printf("[debug] rewrite filter column operation expression=`%s` in logical table `%s`", *op.Value.ConditionExpression, logicalTableID)
							needUpdate = true
						}
						filterColumnOperations[i] = op
					}

					//check project operation
					for i, op := range projectOperations {
						for j, column := range op.Value.ProjectedColumns {
							if column == oldColumnName {
								op.Value.ProjectedColumns[j] = logicalColumnName
								log.Printf("[debug] switch projected columns[%d] `%s` to `%s` in logical table `%s`", i, oldColumnName, logicalColumnName, logicalTableID)
								needUpdate = true
							}
						}
						projectOperations[i] = op
					}

					//check ColumnLevelPermissionRules
					for i, rule := range updateDataSetInput.ColumnLevelPermissionRules {
						for j, columnName := range rule.ColumnNames {
							if columnName == oldColumnName {
								rule.ColumnNames[j] = logicalColumnName
								log.Printf("[debug] change ColumnLevelPermissionRules columns[%d] `%s` to `%s`", j, oldColumnName, logicalColumnName)
							}
						}
						updateDataSetInput.ColumnLevelPermissionRules[i] = rule
					}

				}

				if columnAnnotation.Description == nil {
					log.Printf("[debug] no description phyisical column `%s` in logical table `%s`", physicalColumnName, logicalTableID)
					continue
				}

				// check tag
				if tagColumnOperation, ok := tagColumnOperations[logicalColumnName]; !ok {
					tagColumnOperations[logicalColumnName] = &types.TransformOperationMemberTagColumnOperation{
						Value: types.TagColumnOperation{
							ColumnName: aws.String(logicalColumnName),
							Tags: []types.ColumnTag{
								{
									ColumnDescription: &types.ColumnDescription{
										Text: aws.String(*columnAnnotation.Description),
									},
								},
							},
						},
					}
					log.Printf("[debug] new tag column operation for logical column `%s` in logical table `%s`", logicalColumnName, logicalTableID)
					log.Printf("[info] Update %s (`%s`) field description", logicalColumnName, physicalColumnName)
					needUpdate = true
				} else {
					var exists bool
					for j, tag := range tagColumnOperation.Value.Tags {
						if tag.ColumnDescription == nil {
							continue
						}
						exists = true
						currentDescription := strings.TrimSpace(*tag.ColumnDescription.Text)
						if tag.ColumnDescription.Text != nil && currentDescription != "" {
							if currentDescription != *columnAnnotation.Description && opt.ForceUpdateDescription {
								log.Printf("[debug] keep tag column operation `%s` in logical table `%s`", logicalColumnName, logicalTableID)
								tagColumnOperation.Value.Tags[j].ColumnDescription.Text = aws.String(*columnAnnotation.Description)
								log.Printf("[info] Update %s (`%s`) field description", logicalColumnName, physicalColumnName)
								needUpdate = true
							} else {
								log.Printf("[debug] keep tag column operation `%s` in logical table `%s`", logicalColumnName, logicalTableID)
							}
						} else {
							log.Printf("[debug] tag column operation `%s` is empty, update description in logical table `%s`", logicalColumnName, logicalTableID)
							tagColumnOperation.Value.Tags[j].ColumnDescription.Text = aws.String(*columnAnnotation.Description)
							log.Printf("[info] Update %s (`%s`) field description", logicalColumnName, physicalColumnName)
							needUpdate = true
						}
						break
					}
					if !exists {
						tagColumnOperation.Value.Tags = append(
							tagColumnOperation.Value.Tags,
							types.ColumnTag{
								ColumnDescription: &types.ColumnDescription{
									Text: aws.String(*columnAnnotation.Description),
								},
							},
						)
						log.Printf("[debug] new tag column operation for logical column `%s` in logical table `%s`", logicalColumnName, logicalTableID)
						needUpdate = true
					}
				}
			}
			log.Printf("[debug] rebuild transform operations(rename:%d, tag:%d, others:%d) in logical table `%s`", len(renameColumnOperations), len(tagColumnOperations), len(otherOperations), logicalTableID)
			transformOperations := make([]types.TransformOperation, 0, len(renameColumnOperations)+len(tagColumnOperations)+len(otherOperations)+1)
			transformOperations = append(
				transformOperations,
				lo.Map(
					lo.Values(renameColumnOperations),
					func(op *types.TransformOperationMemberRenameColumnOperation, _ int) types.TransformOperation {
						return op
					},
				)...,
			)
			transformOperations = append(
				transformOperations,
				lo.Map(
					lo.Values(castColumnOperations),
					func(op *types.TransformOperationMemberCastColumnTypeOperation, _ int) types.TransformOperation {
						return op
					},
				)...,
			)
			transformOperations = append(
				transformOperations,
				lo.Map(
					lo.Values(tagColumnOperations),
					func(op *types.TransformOperationMemberTagColumnOperation, _ int) types.TransformOperation {
						return op
					},
				)...,
			)
			transformOperations = append(
				transformOperations,
				lo.Map(
					lo.Values(untagColumnOperations),
					func(op *types.TransformOperationMemberUntagColumnOperation, _ int) types.TransformOperation {
						return op
					},
				)...,
			)
			transformOperations = append(transformOperations, lo.Map(
				createColumnOperations,
				func(op *types.TransformOperationMemberCreateColumnsOperation, _ int) types.TransformOperation {
					return op
				},
			)...)
			transformOperations = append(transformOperations, lo.Map(
				filterColumnOperations,
				func(op *types.TransformOperationMemberFilterOperation, _ int) types.TransformOperation {
					return op
				},
			)...)
			transformOperations = append(transformOperations, lo.Map(
				projectOperations,
				func(op *types.TransformOperationMemberProjectOperation, _ int) types.TransformOperation {
					return op
				},
			)...)
			transformOperations = append(transformOperations, otherOperations...)
			logicalTable.DataTransforms = transformOperations
			updateDataSetInput.LogicalTableMap[logicalTableID] = logicalTable
		}
	}
	if !needUpdate {
		log.Printf("[info] no changes, skip update data set %s", opt.DataSetID)
		return nil
	}
	if opt.Verbose {
		bs, err := json.MarshalIndent(updateDataSetInput, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(app.w, string(bs))
	}
	if opt.DryRun {
		log.Println("[info] *************  end dry run  ****************")
		return nil
	}
	output, err := app.client.UpdateDataSet(ctx, updateDataSetInput)
	if err != nil {
		return fmt.Errorf("UpdateDataSet:%w", err)
	}
	log.Printf("[info] updated data set %s ingestion=`%s`", opt.DataSetID, coalesce(output.IngestionId))
	return nil
}
