package redshiftdatasetannotator

import (
	"context"
	"fmt"
	"log"
)

type ConfigureOption struct {
	Host string `help:"redshift host address" default:""`
	Show bool   `help:"show current configuration" short:"s"`
}

func (app *App) RunConfigure(ctx context.Context, opt *ConfigureOption) error {
	if opt.Show {
		log.Println("configuration file:", configFilePath())
		fmt.Fprintln(app.w, app.cfg.String())
		return nil
	}
	if err := app.cfg.reConfigure(opt.Host); err != nil {
		return err
	}
	return nil
}
