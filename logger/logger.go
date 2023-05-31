package logger

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
)

var log zerolog.Logger

func init() {

	var writers []io.Writer
	env := os.Getenv("APP_ENV")

	if env != "production" {
		writers = append(writers, zerolog.ConsoleWriter{Out: os.Stdout})
	} else {
		writers = append(writers, os.Stdout)

	}
	mw := io.MultiWriter(writers...)

	log = zerolog.New(mw)
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	if env == "production" {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

}

func withContext(ctx context.Context) *zerolog.Logger {

	l := log.With().
		Time("time", time.Now()).
		Logger()

	return &l
}

func Error(ctx context.Context) *zerolog.Event {
	return withContext(ctx).Error().Stack()
}

func ErrorWith(ctx context.Context, err error) *zerolog.Event {
	return withContext(ctx).Error().Stack().Err(err)
}

func Debug(ctx context.Context) *zerolog.Event {
	return withContext(ctx).Debug()
}

func Info(ctx context.Context) *zerolog.Event {
	return withContext(ctx).Info()
}

func Warn(ctx context.Context) *zerolog.Event {
	return withContext(ctx).Warn()
}

func Panic(ctx context.Context) *zerolog.Event {
	return withContext(ctx).Panic()
}
