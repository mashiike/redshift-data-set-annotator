package redshiftdatasetannotator

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/fatih/color"
	"github.com/fujiwara/logutils"
)

type CLI struct {
	AWSAccountID string `help:"QuickSight aws account id"`
	Region       string `help:"AWS region" short:"r" env:"AWS_REGION"`
	LogLevel     string `help:"output log level" env:"LOG_LEVEL" default:"info"`

	Configure *ConfigureOption `cmd:"" help:"Create a configuration file of redshift-data-set-annotator"`
	Annotate  *AnnotateOption  `cmd:"" help:"Annotate a QuickSight dataset with Redshift as the data source"`
	Version   struct{}         `cmd:"" help:"Show version"`
}

func RunCLI(ctx context.Context, args []string) error {
	var cli CLI
	parser, err := kong.New(&cli, kong.Vars{"version": Version})
	if err != nil {
		return err
	}
	kctx, err := parser.Parse(args)
	if err != nil {
		return err
	}
	filter := &logutils.LevelFilter{
		Levels: []logutils.LogLevel{"debug", "info", "notice", "warn", "error"},
		ModifierFuncs: []logutils.ModifierFunc{
			logutils.Color(color.FgHiBlack),
			nil,
			logutils.Color(color.FgHiBlue),
			logutils.Color(color.FgYellow),
			logutils.Color(color.FgRed, color.BgBlack),
		},
		MinLevel: logutils.LogLevel(cli.LogLevel),
		Writer:   os.Stderr,
	}
	log.SetOutput(filter)
	app, err := New(ctx, cli.AWSAccountID)
	if err != nil {
		return err
	}
	cmd := strings.Fields(kctx.Command())[0]
	return app.Dispatch(ctx, cmd, &cli)
}

func (app *App) Dispatch(ctx context.Context, command string, cli *CLI) error {
	switch command {
	case "configure":
		return app.RunConfigure(ctx, cli.Configure)
	case "annotate":
		return app.RunAnnotate(ctx, cli.Annotate)
	case "version":
		fmt.Printf("redshift-data-set-annotator %s\n", Version)
		return nil
	}
	return fmt.Errorf("unknown command: %s", command)
}
