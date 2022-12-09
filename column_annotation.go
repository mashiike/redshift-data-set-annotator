package redshiftdatasetannotator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/jmoiron/sqlx"
	redshiftdatasqldriver "github.com/mashiike/redshift-data-sql-driver"
)

type ColumnAnnotation struct {
	CoumnName   string  `db:"column_name"`
	Name        *string `db:"name"`
	Description *string `db:"description"`
}

type ColumnAnnotations map[string]*ColumnAnnotation

const queryStatement = `
with comments as (
    select
        schemaname
        ,relname as tablename
        ,attname as columnname
        ,trim('\n' from description) as comment
    from pg_stat_user_tables, pg_attribute
    left join pg_description colcom ON pg_attribute.attnum = colcom.objsubid and pg_attribute.attrelid = colcom.objoid
    where pg_attribute.attrelid = pg_stat_user_tables.relid
)
select
    columnname as column_name
    ,trim(split_part(comment,'\n',1)) as name
    ,case when strpos(comment,'\n') > 0 then nullif(trim(substring(comment from strpos(comment,'\n')+1 )),'') end as description
from comments
where schemaname = :schema
    and tablename = :table
`

func (app *App) GetColumnAnnotations(ctx context.Context, ds *types.DataSource, table types.RelationalTable) (ColumnAnnotations, error) {
	parameters, ok := ds.DataSourceParameters.(*types.DataSourceParametersMemberRedshiftParameters)
	if !ok {
		return nil, errors.New("data source is not redshift")
	}
	dsn, err := app.GetDSN(parameters.Value)
	if err != nil {
		return nil, err
	}
	db, err := sqlx.Open("redshift-data", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryxContext(ctx, queryStatement, sql.Named("schema", *table.Schema), sql.Named("table", *table.Name))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	annotations := make(ColumnAnnotations)
	for rows.Next() {
		var annotation ColumnAnnotation
		if err := rows.StructScan(&annotation); err != nil {
			return nil, err
		}
		annotations[annotation.CoumnName] = &annotation
	}
	return annotations, nil
}

func (app *App) GetDSN(params types.RedshiftParameters) (string, error) {
	log.Printf("[debug] connect to redshift host=%s database=%s ",
		coalesce(params.Host),
		coalesce(params.Database),
	)
	host := coalesce(params.Host)
	cfg := &redshiftdatasqldriver.RedshiftDataConfig{
		Database: aws.String(coalesce(params.Database)),
	}

	if isServeless(host) {
		cfg.WorkgroupName = aws.String(getWorkgroupName(host))
		return cfg.String(), nil
	}
	profile, ok := app.cfg.Get(host)
	if !ok {
		profile = app.cfg.GetDefault()
	}
	cfg.DbUser = profile.DBUser
	if cfg.DbUser == nil {
		return "", fmt.Errorf("redshift db user not configured for %s, please execute `redshift-data-set-annotator configure`", host)
	}
	if isProvisoned(host) {
		cfg.ClusterIdentifier = aws.String(getCluseterID(host))
	} else {
		cfg.ClusterIdentifier = profile.ClusterIdentifier
	}
	if cfg.ClusterIdentifier == nil {
		return "", fmt.Errorf("redshift cluster idnetifier not configured for %s, please execute `redshift-data-set-annotator configure`", host)
	}
	return cfg.String(), nil
}
